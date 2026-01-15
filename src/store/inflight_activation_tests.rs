use prost::Message;
use rstest::rstest;
use sqlx::{QueryBuilder, Sqlite};
use std::collections::HashSet;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
use crate::store::inflight_activation::{
    InflightActivationBuilder, InflightActivationStatus, InflightActivationStore,
    InflightActivationStoreConfig, QueryResult, SqliteActivationStore, create_sqlite_pool,
};
use crate::test_utils::{StatusCount, TaskActivationBuilder};
use crate::test_utils::{
    assert_counts, create_integration_config, create_test_store, generate_temp_filename,
    generate_unique_namespace, make_activations, make_activations_with_namespace,
    replace_retry_state,
};
use chrono::{DateTime, SubsecRound, TimeZone, Utc};
use sentry_protos::taskbroker::v1::{
    OnAttemptsExceeded, RetryState, TaskActivation, TaskActivationStatus,
};
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, RetryState, TaskActivationStatus};
use sqlx::{QueryBuilder, Sqlite};
use std::fs;
use tokio::sync::broadcast;
use tokio::task::JoinSet;

#[test]
fn test_inflightactivation_status_is_completion() {
    let mut value = InflightActivationStatus::Unspecified;
    assert!(!value.is_conclusion());

    value = InflightActivationStatus::Pending;
    assert!(!value.is_conclusion());

    value = InflightActivationStatus::Processing;
    assert!(!value.is_conclusion());

    value = InflightActivationStatus::Retry;
    assert!(value.is_conclusion());

    value = InflightActivationStatus::Failure;
    assert!(value.is_conclusion());

    value = InflightActivationStatus::Complete;
    assert!(value.is_conclusion());
}

#[test]
fn test_inflightactivation_status_from() {
    let mut value: InflightActivationStatus = TaskActivationStatus::Pending.into();
    assert_eq!(value, InflightActivationStatus::Pending);

    value = TaskActivationStatus::Processing.into();
    assert_eq!(value, InflightActivationStatus::Processing);

    value = TaskActivationStatus::Retry.into();
    assert_eq!(value, InflightActivationStatus::Retry);

    value = TaskActivationStatus::Failure.into();
    assert_eq!(value, InflightActivationStatus::Failure);

    value = TaskActivationStatus::Complete.into();
    assert_eq!(value, InflightActivationStatus::Complete);
}

#[tokio::test]
async fn test_sqlite_create_db() {
    assert!(
        SqliteActivationStore::new(
            &generate_temp_filename(),
            InflightActivationStoreConfig::from_config(&create_integration_config())
        )
        .await
        .is_ok()
    )
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_store(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(2);
    assert!(store.store(batch).await.is_ok());

    let result = store.count().await;
    assert_eq!(result.unwrap(), 2);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_store_duplicate_id_in_batch(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    // Coerce a conflict
    batch[0].id = "id_0".into();
    batch[1].id = "id_0".into();

    let first_result = store.store(batch).await;
    assert!(
        first_result.is_ok(),
        "{}",
        first_result.err().unwrap().to_string()
    );

    let result = store.count().await;
    assert_eq!(result.unwrap(), 1);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_store_duplicate_id_between_batches(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(2);
    assert!(store.store(batch.clone()).await.is_ok());
    let first_count = store.count().await;
    assert_eq!(first_count.unwrap(), 2);

    let new_batch = make_activations(2);
    // Old batch and new should have conflicts
    assert_eq!(batch[0].id, new_batch[0].id);
    assert_eq!(batch[1].id, new_batch[1].id);
    assert!(store.store(new_batch).await.is_ok());

    let second_count = store.count().await;
    assert_eq!(second_count.unwrap(), 2);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(2);
    assert!(store.store(batch.clone()).await.is_ok());

    let result = store
        .get_pending_activation(None, None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.id, "id_0");
    assert_eq!(result.status, InflightActivationStatus::Processing);
    assert_eq!(result.processing_deadline_duration, 10);
    assert!(
        result.processing_deadline.unwrap().timestamp() >= Utc::now().timestamp() + 13,
        "Should be at least processing_deadline_duration + grace period ahead"
    );
    assert_counts(
        StatusCount {
            pending: 1,
            processing: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_with_race(#[case] adapter: &str) {
    let store = Arc::new(create_test_store(adapter).await);
    let namespace = generate_unique_namespace();

    const NUM_CONCURRENT_WRITES: u32 = 2000;

    for chunk in
        make_activations_with_namespace(namespace.clone(), NUM_CONCURRENT_WRITES).chunks(1024)
    {
        store.store(chunk.to_vec()).await.unwrap();
    }

    let (tx, _) = broadcast::channel::<()>(1);
    let mut join_set = JoinSet::new();

    for _ in 0..NUM_CONCURRENT_WRITES {
        let mut rx = tx.subscribe();
        let store = store.clone();
        let ns = namespace.clone();

        join_set.spawn(async move {
            rx.recv().await.unwrap();
            store
                .get_pending_activation(Some("sentry"), Some(&ns))
                .await
                .unwrap()
                .unwrap()
        });
    }

    tx.send(()).unwrap();

    let res: HashSet<_> = join_set
        .join_all()
        .await
        .iter()
        .map(|ifa| ifa.id.clone())
        .collect();

    assert_eq!(res.len(), NUM_CONCURRENT_WRITES as usize);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_with_namespace(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[1].namespace = "other_namespace".into();
    assert!(store.store(batch.clone()).await.is_ok());

    // Get activation from other namespace
    let result = store
        .get_pending_activation(Some("sentry"), Some("other_namespace"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.id, "id_1");
    assert_eq!(result.status, InflightActivationStatus::Processing);
    assert!(result.processing_deadline.unwrap() > Utc::now());
    assert_eq!(result.namespace, "other_namespace");
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_from_multiple_namespaces(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(4);
    batch[0].namespace = "ns1".into();
    batch[1].namespace = "ns2".into();
    batch[2].namespace = "ns3".into();
    batch[3].namespace = "ns4".into();
    assert!(store.store(batch.clone()).await.is_ok());

    // Get activation from multiple namespaces (should get oldest)
    let namespaces = vec!["ns2".to_string(), "ns3".to_string()];
    let result = store
        .get_pending_activations_from_namespaces(None, Some(&namespaces), None)
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[1].id, "id_2");
    assert_eq!(result[1].namespace, "ns3");
    assert_eq!(result[1].status, InflightActivationStatus::Processing);
    assert_eq!(result[0].id, "id_1");
    assert_eq!(result[0].namespace, "ns2");
    assert_eq!(result[0].status, InflightActivationStatus::Processing);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_with_namespace_requires_application(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[1].namespace = "other_namespace".into();
    assert!(store.store(batch.clone()).await.is_ok());

    // This is an invalid query as we don't want to allow clients
    // to fetch tasks from any application.
    let opt = store
        .get_pending_activation(None, Some("other_namespace"))
        .await
        .unwrap();
    assert!(opt.is_none());

    // We allow no application in this method because of usage in upkeep
    let namespaces = vec!["other_namespace".to_string()];
    let activations = store
        .get_pending_activations_from_namespaces(None, Some(namespaces).as_deref(), Some(2))
        .await
        .unwrap();
    assert_eq!(
        1,
        activations.len(),
        "should find 1 activation with a matching namespace"
    );
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_skip_expires(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    assert_counts(
        StatusCount {
            pending: 0,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let mut batch = make_activations(1);
    batch[0].expires_at = Some(Utc::now() - Duration::from_secs(100));
    assert!(store.store(batch.clone()).await.is_ok());

    let result = store.get_pending_activation(None, None).await;
    assert!(result.is_ok());
    let res_option = result.unwrap();
    assert!(res_option.is_none());

    assert_counts(
        StatusCount {
            pending: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_earliest(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[0].added_at = Utc.with_ymd_and_hms(2024, 6, 24, 0, 0, 0).unwrap();
    batch[1].added_at = Utc.with_ymd_and_hms(1998, 6, 24, 0, 0, 0).unwrap();
    let ret = store.store(batch.clone()).await;
    assert!(ret.is_ok(), "{}", ret.err().unwrap().to_string());

    let result = store
        .get_pending_activation(None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result.added_at,
        Utc.with_ymd_and_hms(1998, 6, 24, 0, 0, 0).unwrap()
    );
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_fetches_application(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(1);
    batch[0].application = "hammers".into();
    assert!(store.store(batch.clone()).await.is_ok());

    // Getting an activation with no application filter should
    // include activations with application set.
    let result = store
        .get_pending_activation(None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.id, "id_0");
    assert_eq!(result.status, InflightActivationStatus::Processing);
    assert!(result.processing_deadline.unwrap() > Utc::now());
    assert_eq!(result.application, "hammers");
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_with_application(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[1].application = "hammers".into();
    assert!(store.store(batch.clone()).await.is_ok());

    // Get activation from a named application
    let result = store
        .get_pending_activation(Some("hammers"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.id, "id_1");
    assert_eq!(result.status, InflightActivationStatus::Processing);
    assert!(result.processing_deadline.unwrap() > Utc::now());
    assert_eq!(result.application, "hammers");

    let result_opt = store
        .get_pending_activation(Some("hammers"), None)
        .await
        .unwrap();
    assert!(
        result_opt.is_none(),
        "no pending activations in hammers left"
    );

    let result_opt = store.get_pending_activation(None, None).await.unwrap();
    assert!(result_opt.is_some(), "one pending activation in '' left");
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_pending_activation_with_application_and_namespace(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(3);
    batch[0].namespace = "target".into();

    batch[1].application = "hammers".into();
    batch[1].namespace = "target".into();

    batch[2].application = "hammers".into();
    batch[2].namespace = "not-target".into();
    assert!(store.store(batch.clone()).await.is_ok());

    // Get activation from a named application
    let result = store
        .get_pending_activation(Some("hammers"), Some("target"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.id, "id_1");
    assert_eq!(result.status, InflightActivationStatus::Processing);
    assert!(result.processing_deadline.unwrap() > Utc::now());
    assert_eq!(result.application, "hammers");
    assert_eq!(result.namespace, "target");

    let result = store
        .get_pending_activation(Some("hammers"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.id, "id_2");
    assert_eq!(result.application, "hammers");
    assert_eq!(result.namespace, "not-target");
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_count_pending_activations(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(3);
    batch[0].status = InflightActivationStatus::Processing;
    assert!(store.store(batch).await.is_ok());

    assert_eq!(store.count_pending_activations().await.unwrap(), 2);
    assert_counts(
        StatusCount {
            pending: 2,
            processing: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_set_activation_status(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(2);
    assert!(store.store(batch).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    assert!(
        store
            .set_status("id_0", InflightActivationStatus::Failure)
            .await
            .is_ok()
    );
    assert_counts(
        StatusCount {
            pending: 1,
            failure: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    assert!(
        store
            .set_status("id_0", InflightActivationStatus::Pending)
            .await
            .is_ok()
    );
    assert_counts(
        StatusCount {
            pending: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    assert!(
        store
            .set_status("id_0", InflightActivationStatus::Failure)
            .await
            .is_ok()
    );
    assert!(
        store
            .set_status("id_1", InflightActivationStatus::Failure)
            .await
            .is_ok()
    );
    assert_counts(
        StatusCount {
            pending: 0,
            failure: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    assert!(
        store
            .get_pending_activation(None, None)
            .await
            .unwrap()
            .is_none()
    );

    let result = store
        .set_status("not_there", InflightActivationStatus::Complete)
        .await;
    assert!(result.is_ok(), "no query error");

    let activation = result.unwrap();
    assert!(activation.is_none(), "no activation found");

    let result = store
        .set_status("id_0", InflightActivationStatus::Complete)
        .await;
    assert!(result.is_ok(), "no query error");

    let result_opt = result.unwrap();
    assert!(result_opt.is_some(), "activation should be returned");
    let inflight = result_opt.unwrap();
    assert_eq!(inflight.id, "id_0");
    assert_eq!(inflight.status, InflightActivationStatus::Complete);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_set_processing_deadline(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(1);
    assert!(store.store(batch.clone()).await.is_ok());

    let deadline = Utc::now().round_subsecs(0);
    let result = store.set_processing_deadline("id_0", Some(deadline)).await;
    assert!(result.is_ok(), "query error: {:?}", result.err().unwrap());

    let result = store.get_by_id("id_0").await.unwrap().unwrap();
    assert_eq!(
        result.processing_deadline.unwrap().timestamp(),
        deadline.timestamp()
    );
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_delete_activation(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(2);
    assert!(store.store(batch).await.is_ok());

    let result = store.count().await;
    assert_eq!(result.unwrap(), 2);

    assert!(store.delete_activation("id_0").await.is_ok());
    let result = store.count().await;
    assert_eq!(result.unwrap(), 1);

    assert!(store.delete_activation("id_0").await.is_ok());
    let result = store.count().await;
    assert_eq!(result.unwrap(), 1);

    assert!(store.delete_activation("id_1").await.is_ok());
    let result = store.count().await;
    assert_eq!(result.unwrap(), 0);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_get_retry_activations(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(2);
    assert!(store.store(batch.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    assert!(
        store
            .set_status("id_0", InflightActivationStatus::Retry)
            .await
            .is_ok()
    );
    assert_counts(
        StatusCount {
            pending: 1,
            retry: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    assert!(
        store
            .set_status("id_1", InflightActivationStatus::Retry)
            .await
            .is_ok()
    );

    let retries = store.get_retry_activations().await.unwrap();
    assert_eq!(retries.len(), 2);
    for record in retries.iter() {
        assert_eq!(record.status, InflightActivationStatus::Retry);
    }
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_handle_processing_deadline(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());

    assert!(store.store(batch.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 1,
            processing: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);
    assert_counts(
        StatusCount {
            pending: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let task = store.get_by_id(&batch[1].id).await;
    assert_eq!(task.unwrap().unwrap().processing_attempts, 1);

    // Run again to check early return
    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 0);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_handle_processing_deadline_multiple_tasks(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[0].status = InflightActivationStatus::Processing;
    batch[0].processing_deadline = Some(Utc.with_ymd_and_hms(2020, 1, 1, 1, 1, 1).unwrap());
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc::now() + chrono::Duration::days(30));
    assert!(store.store(batch).await.is_ok());
    assert_counts(
        StatusCount {
            processing: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);
    assert_counts(
        StatusCount {
            pending: 1,
            processing: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_handle_processing_at_most_once(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    // Both records are past processing deadlines
    let mut batch = make_activations(2);
    batch[0].status = InflightActivationStatus::Processing;
    batch[0].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());

    batch[1].status = InflightActivationStatus::Processing;
    replace_retry_state(
        &mut batch[1],
        Some(RetryState {
            attempts: 0,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
            at_most_once: Some(true),
            delay_on_retry: None,
        }),
    );
    batch[1].at_most_once = true;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());

    assert!(store.store(batch.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            processing: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 2);
    assert_counts(
        StatusCount {
            pending: 1,
            failure: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let task = store.get_by_id(&batch[1].id).await.unwrap().unwrap();
    assert_eq!(task.status, InflightActivationStatus::Failure);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_handle_processing_deadline_discard_after(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
    replace_retry_state(
        &mut batch[1],
        Some(RetryState {
            attempts: 0,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
            at_most_once: None,
            delay_on_retry: None,
        }),
    );

    assert!(store.store(batch).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 1,
            processing: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);
    assert_counts(
        StatusCount {
            pending: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_handle_processing_deadline_deadletter_after(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
    replace_retry_state(
        &mut batch[1],
        Some(RetryState {
            attempts: 0,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
            at_most_once: None,
            delay_on_retry: None,
        }),
    );

    assert!(store.store(batch).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 1,
            processing: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);
    assert_counts(
        StatusCount {
            pending: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_handle_processing_deadline_no_retries_remaining(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(2);
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
    replace_retry_state(
        &mut batch[1],
        Some(RetryState {
            attempts: 1,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
            at_most_once: None,
            delay_on_retry: None,
        }),
    );
    assert!(store.store(batch).await.is_ok());
    assert_counts(
        StatusCount {
            processing: 1,
            pending: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);
    assert_counts(
        StatusCount {
            processing: 0,
            pending: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_processing_attempts_exceeded(#[case] adapter: &str) {
    let config = create_integration_config();
    let store = create_test_store(adapter).await;

    let mut batch = make_activations(3);
    batch[0].status = InflightActivationStatus::Pending;
    batch[0].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
    batch[0].processing_attempts = config.max_processing_attempts as i32;

    batch[1].status = InflightActivationStatus::Complete;
    batch[1].added_at += Duration::from_secs(1);

    batch[2].status = InflightActivationStatus::Pending;
    batch[2].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
    batch[2].processing_attempts = config.max_processing_attempts as i32;

    assert!(store.store(batch.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            complete: 1,
            pending: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let count = store.handle_processing_attempts().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 2);
    assert_counts(
        StatusCount {
            complete: 1,
            failure: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_remove_completed(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut records = make_activations(3);
    records[0].status = InflightActivationStatus::Complete;
    records[1].status = InflightActivationStatus::Pending;
    records[1].added_at += Duration::from_secs(1);
    records[2].status = InflightActivationStatus::Complete;
    records[2].added_at += Duration::from_secs(2);

    assert!(store.store(records.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            complete: 2,
            pending: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let result = store.remove_completed().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);
    assert!(
        store
            .get_by_id(&records[0].id)
            .await
            .expect("no error")
            .is_none()
    );
    assert!(
        store
            .get_by_id(&records[1].id)
            .await
            .expect("no error")
            .is_some()
    );
    assert!(
        store
            .get_by_id(&records[2].id)
            .await
            .expect("no error")
            .is_none()
    );
    assert_counts(
        StatusCount {
            complete: 0,
            pending: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_remove_completed_multiple_gaps(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut records = make_activations(4);
    // only record 1 can be removed
    records[0].status = InflightActivationStatus::Complete;
    records[1].status = InflightActivationStatus::Failure;
    records[1].added_at += Duration::from_secs(1);

    records[2].status = InflightActivationStatus::Complete;
    records[2].added_at += Duration::from_secs(2);

    records[3].status = InflightActivationStatus::Processing;
    records[3].added_at += Duration::from_secs(3);

    assert!(store.store(records.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            complete: 2,
            processing: 1,
            failure: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let result = store.remove_completed().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);
    assert!(
        store
            .get_by_id(&records[0].id)
            .await
            .expect("no error")
            .is_none()
    );
    assert!(
        store
            .get_by_id(&records[1].id)
            .await
            .expect("no error")
            .is_some()
    );
    assert!(
        store
            .get_by_id(&records[2].id)
            .await
            .expect("no error")
            .is_none()
    );
    assert!(
        store
            .get_by_id(&records[3].id)
            .await
            .expect("no error")
            .is_some()
    );
    assert_counts(
        StatusCount {
            complete: 0,
            processing: 1,
            failure: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_handle_failed_tasks(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let mut records = make_activations(4);
    // deadletter
    records[0].status = InflightActivationStatus::Failure;
    replace_retry_state(
        &mut records[0],
        Some(RetryState {
            attempts: 1,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
            at_most_once: None,
            delay_on_retry: None,
        }),
    );
    // discard
    records[1].status = InflightActivationStatus::Failure;
    replace_retry_state(
        &mut records[1],
        Some(RetryState {
            attempts: 1,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
            at_most_once: None,
            delay_on_retry: None,
        }),
    );
    // no retry state = discard
    records[2].status = InflightActivationStatus::Failure;
    replace_retry_state(&mut records[2], None);

    // Another deadletter
    records[3].status = InflightActivationStatus::Failure;
    replace_retry_state(
        &mut records[3],
        Some(RetryState {
            attempts: 1,
            max_attempts: 1,
            on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
            at_most_once: None,
            delay_on_retry: None,
        }),
    );
    assert!(store.store(records.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            failure: 4,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let result = store.handle_failed_tasks().await;
    assert!(result.is_ok(), "handle_failed_tasks should be ok");
    let fowarder = result.unwrap();

    assert_eq!(
        fowarder.to_deadletter.len(),
        2,
        "should have two tasks to deadletter"
    );
    assert!(
        store.get_by_id(&fowarder.to_deadletter[0].0).await.is_ok(),
        "deadletter records still in sqlite"
    );
    assert!(
        store.get_by_id(&fowarder.to_deadletter[1].0).await.is_ok(),
        "deadletter records still in sqlite"
    );
    assert_eq!(fowarder.to_deadletter[0].0, records[0].id);
    assert_eq!(fowarder.to_deadletter[1].0, records[3].id);

    assert_counts(
        StatusCount {
            failure: 2,
            complete: 2,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_mark_completed(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let records = make_activations(3);
    assert!(store.store(records.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 3,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let ids: Vec<String> = records.iter().map(|item| item.id.clone()).collect();
    let result = store.mark_completed(ids.clone()).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3, "three records updated");
    // No pending tasks left
    // All tasks should be complete
    assert_counts(
        StatusCount {
            complete: 3,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_handle_expires_at(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let mut batch = make_activations(3);

    // All expired tasks should be removed, regardless of order or other tasks.
    batch[0].expires_at = Some(Utc::now() - (Duration::from_secs(5 * 60)));
    batch[1].expires_at = Some(Utc::now() + (Duration::from_secs(5 * 60)));
    batch[2].expires_at = Some(Utc::now() - (Duration::from_secs(5 * 60)));

    assert!(store.store(batch.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 3,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let result = store.handle_expires_at().await;
    assert!(
        result.is_ok(),
        "handle_expires_at should be ok {:?}",
        result
    );
    assert_eq!(result.unwrap(), 2);
    assert_counts(
        StatusCount {
            pending: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_remove_killswitched(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    let mut batch = make_activations(6);

    batch[0].taskname = "task_to_be_killswitched_one".to_string();
    batch[2].taskname = "task_to_be_killswitched_two".to_string();
    batch[4].taskname = "task_to_be_killswitched_three".to_string();

    assert!(store.store(batch.clone()).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 6,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;

    let result = store
        .remove_killswitched(vec![
            "task_to_be_killswitched_one".to_string(),
            "task_to_be_killswitched_two".to_string(),
            "task_to_be_killswitched_three".to_string(),
        ])
        .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3);
    assert_counts(
        StatusCount {
            pending: 3,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_clear(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let received_at = DateTime::from_timestamp_nanos(0);
    let expires_at = received_at + Duration::from_secs(1);

    let namespace = generate_unique_namespace();

    let batch = vec![
        InflightActivationBuilder::new()
            .id("id_0")
            .taskname("taskname")
            .namespace(&namespace)
            .received_at(received_at)
            .expires_at(expires_at)
            .build(TaskActivationBuilder::new()),
    ];

    assert!(store.store(batch).await.is_ok());
    assert_counts(
        StatusCount {
            pending: 1,
            ..StatusCount::default()
        },
        store.as_ref(),
    )
    .await;
    assert_eq!(store.count().await.unwrap(), 1);

    assert!(store.clear().await.is_ok());
    assert_eq!(store.count().await.unwrap(), 0);
    assert_counts(StatusCount::default(), store.as_ref()).await;
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_full_vacuum(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(2);
    assert!(store.store(batch).await.is_ok());

    let result = store.full_vacuum_db().await;
    assert!(result.is_ok());
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_vacuum_db_no_limit(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;

    let batch = make_activations(2);
    assert!(store.store(batch).await.is_ok());

    let result = store.vacuum_db().await;
    assert!(result.is_ok());
    store.remove_db().await.unwrap();
}

#[tokio::test]
async fn test_vacuum_db_incremental() {
    let config = Config {
        vacuum_page_count: Some(10),
        ..Config::default()
    };
    let store = SqliteActivationStore::new(
        &generate_temp_filename(),
        InflightActivationStoreConfig::from_config(&config),
    )
    .await
    .expect("could not create store");

    let batch = make_activations(2);
    assert!(store.store(batch).await.is_ok());

    let result = store.vacuum_db().await;
    assert!(result.is_ok());
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_db_size(#[case] adapter: &str) {
    let store = create_test_store(adapter).await;
    assert!(store.db_size().await.is_ok());

    let first_size = store.db_size().await.unwrap();
    assert!(first_size > 0, "should have some bytes");

    // Generate a large enough batch that we use another page.
    let batch = make_activations(50);
    assert!(store.store(batch).await.is_ok());

    let second_size = store.db_size().await.unwrap();
    assert!(second_size > first_size, "should have more bytes now");
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_pending_activation_max_lag_no_pending(#[case] adapter: &str) {
    let now = Utc::now();
    let store = create_test_store(adapter).await;
    // No activations, max lag is 0
    assert_eq!(0.0, store.pending_activation_max_lag(&now).await);

    let mut processing = make_activations(1);
    processing[0].status = InflightActivationStatus::Processing;
    assert!(store.store(processing).await.is_ok());

    // No pending activations, max lag is 0
    assert_eq!(0.0, store.pending_activation_max_lag(&now).await);
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_pending_activation_max_lag_use_oldest(#[case] adapter: &str) {
    let now = Utc::now();
    let store = create_test_store(adapter).await;

    let mut pending = make_activations(2);
    pending[0].received_at = now - Duration::from_secs(10);
    pending[1].received_at = now - Duration::from_secs(500);
    assert!(store.store(pending).await.is_ok());

    let result = store.pending_activation_max_lag(&now).await;
    assert!(11.0 < result, "Should not get the small record");
    assert!(result < 501.0, "Should not get an inflated value");
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_pending_activation_max_lag_ignore_processing_attempts(#[case] adapter: &str) {
    let now = Utc::now().round_subsecs(0);
    let store = create_test_store(adapter).await;

    let mut pending = make_activations(2);
    pending[0].received_at = now - Duration::from_secs(10);
    pending[1].received_at = now - Duration::from_secs(500);
    pending[1].processing_attempts = 1;
    assert!(store.store(pending).await.is_ok());

    let result = store.pending_activation_max_lag(&now).await;
    assert_eq!(result, 10.0, "max lag: {result:?}");
    store.remove_db().await.unwrap();
}

#[tokio::test]
#[rstest]
#[case::sqlite("sqlite")]
#[case::postgres("postgres")]
async fn test_pending_activation_max_lag_account_for_delayed(#[case] adapter: &str) {
    let now = Utc::now();
    let store = create_test_store(adapter).await;

    let mut pending = make_activations(2);
    // delayed tasks are received well before they become pending
    // the lag of a delayed task should begin *after* the delay has passed.
    pending[0].received_at = now - Duration::from_secs(520);
    pending[0].delay_until = Some(now - Duration::from_millis(22020));
    assert!(store.store(pending).await.is_ok());

    let result = store.pending_activation_max_lag(&now).await;
    assert!(22.00 < result, "result: {result}");
    assert!(result < 24.00, "result: {result}");
    store.remove_db().await.unwrap();
}

#[tokio::test]
async fn test_db_status_calls_ok() {
    use libsqlite3_sys::{
        SQLITE_DBSTATUS_CACHE_USED, SQLITE_DBSTATUS_SCHEMA_USED, SQLITE_OK, sqlite3_db_status,
    };
    use std::time::SystemTime;

    // Create a unique on-disk database URL
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = format!("/tmp/taskbroker-dbstatus-{nanos}.sqlite");
    let url = format!("sqlite:{db_path}");

    // Initialize a store to create the database and run migrations
    SqliteActivationStore::new(
        &url,
        InflightActivationStoreConfig {
            max_processing_attempts: 3,
            processing_deadline_grace_sec: 0,
            vacuum_page_count: None,
            enable_sqlite_status_metrics: false,
        },
    )
    .await
    .expect("store init");

    // Acquire a fresh read connection from a temporary pool, since store.read_pool is private
    let (read_pool, _write_pool) = create_sqlite_pool(&url).await.expect("pool");
    let mut conn = read_pool.acquire().await.expect("acquire read conn");
    let mut raw = conn.lock_handle().await.expect("lock_handle");

    let mut cur: i32 = 0;
    let mut hi: i32 = 0;

    unsafe {
        // Should succeed and write some non-negative values
        let rc = sqlite3_db_status(
            raw.as_raw_handle().as_mut(),
            SQLITE_DBSTATUS_CACHE_USED,
            &mut cur,
            &mut hi,
            0,
        );
        assert_eq!(rc, SQLITE_OK);
        assert!(cur >= 0);

        let rc2 = sqlite3_db_status(
            raw.as_raw_handle().as_mut(),
            SQLITE_DBSTATUS_SCHEMA_USED,
            &mut cur,
            &mut hi,
            0,
        );
        assert_eq!(rc2, SQLITE_OK);
        assert!(cur >= 0);
    }
}

struct TestFolders {
    parent_folder: String,
    initial_folder: String,
    other_folder: String,
}

impl TestFolders {
    fn new() -> Result<Self, Error> {
        let parent_folder = "./testmigrations".to_string();
        let parent = fs::create_dir(&parent_folder);
        if parent.is_err() {
            return Err(parent.err().unwrap());
        }

        let initial_folder = parent_folder.clone() + "/initial_migrations";
        let other_folder = parent_folder.clone() + "/other_migrations";

        let initial = fs::create_dir(&initial_folder);
        if initial.is_err() {
            return Err(initial.err().unwrap());
        }
        let other = fs::create_dir(&other_folder);
        if other.is_err() {
            return Err(other.err().unwrap());
        }

        Ok(TestFolders {
            parent_folder,
            initial_folder,
            other_folder,
        })
    }
}

impl Drop for TestFolders {
    fn drop(&mut self) {
        let parent = fs::remove_dir_all(Path::new(&self.parent_folder));
        if parent.is_err() {
            println!("Could not remove dir {}, {:?}", self.parent_folder, parent);
        }
    }
}

#[tokio::test]
async fn test_migrations() {
    // Create the folders that will be used
    let folders = TestFolders::new().unwrap();

    // Move migrations to different folders
    let orig = fs::read_dir("./migrations");
    assert!(orig.is_ok(), "{orig:?}");

    let origdir = orig.unwrap();
    for result in origdir {
        assert!(result.is_ok(), "{result:?}");
        let entry = result.unwrap();
        let filename = entry.file_name().into_string().unwrap();
        // Write the initial migration to a separate folder, so the table can be initialized without any migrations.
        if filename.starts_with("0001") {
            let result = fs::copy(
                entry.path(),
                folders.initial_folder.clone() + "/" + &filename,
            );
            assert!(result.is_ok(), "{result:?}");
        }

        let result = fs::copy(entry.path(), folders.other_folder.clone() + "/" + &filename);
        assert!(result.is_ok(), "{result:?}");
    }

    // Run initial migration
    let (_read_pool, write_pool) = create_sqlite_pool(&generate_temp_filename()).await.unwrap();
    let result = sqlx::migrate::Migrator::new(Path::new(&folders.initial_folder))
        .await
        .unwrap()
        .run(&write_pool)
        .await;
    assert!(result.is_ok(), "{result:?}");

    // Insert rows. Note that this query lines up with the 0001 migration table.
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
        INSERT INTO inflight_taskactivations
            (
                id,
                activation,
                partition,
                offset,
                added_at,
                processing_attempts,
                expires_at,
                processing_deadline_duration,
                processing_deadline,
                status,
                at_most_once
            )
        ",
    );
    let activations = make_activations(2);
    let query = query_builder
        .push_values(activations, |mut b, row| {
            b.push_bind(row.id);
            b.push_bind(row.activation);
            b.push_bind(row.partition);
            b.push_bind(row.offset);
            b.push_bind(row.added_at.timestamp());
            b.push_bind(row.processing_attempts);
            b.push_bind(row.expires_at.map(|t| Some(t.timestamp())));
            b.push_bind(row.processing_deadline_duration);
            if let Some(deadline) = row.processing_deadline {
                b.push_bind(deadline.timestamp());
            } else {
                // Add a literal null
                b.push("null");
            }
            b.push_bind(row.status);
            b.push_bind(row.at_most_once);
        })
        .push(" ON CONFLICT(id) DO NOTHING")
        .build();
    let result = query.execute(&write_pool).await;
    assert!(result.is_ok(), "{result:?}");
    let meta_result: QueryResult = result.unwrap().into();
    assert_eq!(meta_result.rows_affected, 2);

    // Run other migrations
    let result = sqlx::migrate::Migrator::new(Path::new(&folders.other_folder))
        .await
        .unwrap()
        .run(&write_pool)
        .await;
    assert!(result.is_ok(), "{result:?}");
}

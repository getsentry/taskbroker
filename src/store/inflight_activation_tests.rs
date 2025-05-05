use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{SubsecRound, TimeZone, Utc};
use sentry_protos::taskbroker::v1::{
    OnAttemptsExceeded, RetryState, TaskActivation, TaskActivationStatus,
};
use tokio::sync::broadcast;
use tokio::task::JoinSet;

use crate::store::inflight_activation::{
    InflightActivation, InflightActivationStatus, InflightActivationStore,
    InflightActivationStoreConfig,
};
use crate::test_utils::{
    assert_count_by_status, create_integration_config, create_test_store, generate_temp_filename,
    make_activations,
};

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
async fn test_create_db() {
    assert!(
        InflightActivationStore::new(
            &generate_temp_filename(),
            InflightActivationStoreConfig::from_config(&create_integration_config())
        )
        .await
        .is_ok()
    )
}

#[tokio::test]
async fn test_store() {
    let store = create_test_store().await;

    let batch = make_activations(2);
    assert!(store.store(batch).await.is_ok());

    let result = store.count().await;
    assert_eq!(result.unwrap(), 2);
}

#[tokio::test]
async fn test_store_duplicate_id_in_batch() {
    let store = create_test_store().await;

    let mut batch = make_activations(2);
    // Coerce a conflict
    batch[0].activation.id = "id_0".into();
    batch[1].activation.id = "id_0".into();

    assert!(store.store(batch).await.is_ok());

    let result = store.count().await;
    assert_eq!(result.unwrap(), 1);
}

#[tokio::test]
async fn test_store_duplicate_id_between_batches() {
    let store = create_test_store().await;

    let batch = make_activations(2);
    assert!(store.store(batch.clone()).await.is_ok());
    let first_count = store.count().await;
    assert_eq!(first_count.unwrap(), 2);

    let new_batch = make_activations(2);
    // Old batch and new should have conflicts
    assert_eq!(batch[0].activation.id, new_batch[0].activation.id);
    assert_eq!(batch[1].activation.id, new_batch[1].activation.id);
    assert!(store.store(new_batch).await.is_ok());

    let second_count = store.count().await;
    assert_eq!(second_count.unwrap(), 2);
}

#[tokio::test]
async fn test_get_pending_activation() {
    let store = create_test_store().await;

    let batch = make_activations(2);
    assert!(store.store(batch.clone()).await.is_ok());

    let result = store.get_pending_activation(None).await.unwrap().unwrap();

    assert_eq!(result.activation.id, "id_0");
    assert_eq!(result.status, InflightActivationStatus::Processing);
    assert!(result.processing_deadline.unwrap() > Utc::now());
    assert_count_by_status(&store, InflightActivationStatus::Pending, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn test_get_pending_activation_with_race() {
    let store = Arc::new(create_test_store().await);

    const NUM_CONCURRENT_WRITES: u32 = 2000;

    for chunk in make_activations(NUM_CONCURRENT_WRITES).chunks(1024) {
        store.store(chunk.to_vec()).await.unwrap();
    }

    let (tx, _) = broadcast::channel::<()>(1);
    let mut join_set = JoinSet::new();

    for _ in 0..NUM_CONCURRENT_WRITES {
        let mut rx = tx.subscribe();
        let store = store.clone();
        join_set.spawn(async move {
            rx.recv().await.unwrap();
            store
                .get_pending_activation(Some("namespace"))
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
        .map(|ifa| ifa.activation.id.clone())
        .collect();

    assert_eq!(res.len(), NUM_CONCURRENT_WRITES as usize);
}

#[tokio::test]
async fn test_get_pending_activation_with_namespace() {
    let store = create_test_store().await;

    let mut batch = make_activations(2);
    batch[1].namespace = "other_namespace".into();
    assert!(store.store(batch.clone()).await.is_ok());

    // Get activation from other namespace
    let result = store
        .get_pending_activation(Some("other_namespace"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.activation.id, "id_1");
    assert_eq!(result.status, InflightActivationStatus::Processing);
    assert!(result.processing_deadline.unwrap() > Utc::now());
    assert_eq!(result.namespace, "other_namespace");
}

#[tokio::test]
async fn test_get_pending_activation_skip_expires() {
    let store = create_test_store().await;

    let mut batch = make_activations(1);
    batch[0].expires_at = Some(Utc::now() - Duration::from_secs(100));
    assert!(store.store(batch.clone()).await.is_ok());

    let result = store.get_pending_activation(None).await;
    assert!(result.is_ok());
    let res_option = result.unwrap();
    assert!(res_option.is_none());
    assert_count_by_status(&store, InflightActivationStatus::Pending, 1).await;
}

#[tokio::test]
async fn test_get_pending_activation_earliest() {
    let store = create_test_store().await;

    let mut batch = make_activations(2);
    batch[0].added_at = Utc.with_ymd_and_hms(2024, 6, 24, 0, 0, 0).unwrap();
    batch[1].added_at = Utc.with_ymd_and_hms(1998, 6, 24, 0, 0, 0).unwrap();
    assert!(store.store(batch.clone()).await.is_ok());

    let result = store.get_pending_activation(None).await.unwrap().unwrap();
    assert_eq!(
        result.added_at,
        Utc.with_ymd_and_hms(1998, 6, 24, 0, 0, 0).unwrap()
    );
}

#[tokio::test]
async fn test_count_pending_activations() {
    let store = create_test_store().await;

    let mut batch = make_activations(3);
    batch[0].status = InflightActivationStatus::Processing;
    assert!(store.store(batch).await.is_ok());

    assert_eq!(store.count_pending_activations().await.unwrap(), 2);

    assert_count_by_status(&store, InflightActivationStatus::Pending, 2).await;
}

#[tokio::test]
async fn set_activation_status() {
    let store = create_test_store().await;

    let batch = make_activations(2);
    assert!(store.store(batch).await.is_ok());

    assert_eq!(store.count_pending_activations().await.unwrap(), 2);
    assert!(
        store
            .set_status("id_0", InflightActivationStatus::Failure)
            .await
            .is_ok()
    );
    assert_eq!(store.count_pending_activations().await.unwrap(), 1);
    assert!(
        store
            .set_status("id_0", InflightActivationStatus::Pending)
            .await
            .is_ok()
    );
    assert_eq!(store.count_pending_activations().await.unwrap(), 2);
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
    assert_eq!(store.count_pending_activations().await.unwrap(), 0);
    assert!(store.get_pending_activation(None).await.unwrap().is_none());

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
    assert_eq!(inflight.activation.id, "id_0");
    assert_eq!(inflight.status, InflightActivationStatus::Complete);
}

#[tokio::test]
async fn test_set_processing_deadline() {
    let store = create_test_store().await;

    let batch = make_activations(1);
    assert!(store.store(batch.clone()).await.is_ok());

    let deadline = Utc::now();
    assert!(
        store
            .set_processing_deadline("id_0", Some(deadline))
            .await
            .is_ok()
    );

    let result = store.get_by_id("id_0").await.unwrap().unwrap();
    assert_eq!(
        result
            .processing_deadline
            .unwrap()
            .round_subsecs(0)
            .timestamp(),
        deadline.timestamp()
    )
}

#[tokio::test]
async fn test_delete_activation() {
    let store = create_test_store().await;

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
}

#[tokio::test]
async fn test_get_retry_activations() {
    let store = create_test_store().await;

    let batch = make_activations(2);
    assert!(store.store(batch.clone()).await.is_ok());

    assert_count_by_status(&store, InflightActivationStatus::Pending, 2).await;

    assert!(
        store
            .set_status("id_0", InflightActivationStatus::Retry)
            .await
            .is_ok()
    );
    assert_count_by_status(&store, InflightActivationStatus::Pending, 1).await;

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
}

#[tokio::test]
async fn test_handle_processing_deadline() {
    let store = create_test_store().await;

    let mut batch = make_activations(2);
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());

    assert!(store.store(batch.clone()).await.is_ok());

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);

    assert_count_by_status(&store, InflightActivationStatus::Processing, 0).await;
    assert_count_by_status(&store, InflightActivationStatus::Pending, 2).await;

    let task = store.get_by_id(&batch[1].activation.id).await;
    assert_eq!(task.unwrap().unwrap().processing_attempts, 1);

    // Run again to check early return
    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 0);
}

#[tokio::test]
async fn test_handle_processing_deadline_multiple_tasks() {
    let store = create_test_store().await;

    let mut batch = make_activations(2);
    batch[0].status = InflightActivationStatus::Processing;
    batch[0].processing_deadline = Some(Utc.with_ymd_and_hms(2020, 1, 1, 1, 1, 1).unwrap());
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc::now() + chrono::Duration::days(30));
    assert!(store.store(batch).await.is_ok());

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);

    assert_count_by_status(&store, InflightActivationStatus::Processing, 1).await;
    assert_count_by_status(&store, InflightActivationStatus::Pending, 1).await;
}

#[tokio::test]
async fn test_handle_processing_at_most_once() {
    let store = create_test_store().await;

    // Both records are past processing deadlines
    let mut batch = make_activations(2);
    batch[0].status = InflightActivationStatus::Processing;
    batch[0].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());

    batch[1].status = InflightActivationStatus::Processing;
    batch[1].activation.retry_state = Some(RetryState {
        attempts: 0,
        max_attempts: 1,
        on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
        at_most_once: Some(true),
    });
    batch[1].at_most_once = true;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());

    assert!(store.store(batch.clone()).await.is_ok());

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 2);

    assert_count_by_status(&store, InflightActivationStatus::Processing, 0).await;
    assert_count_by_status(&store, InflightActivationStatus::Pending, 1).await;
    assert_count_by_status(&store, InflightActivationStatus::Failure, 1).await;

    let task = store
        .get_by_id(&batch[1].activation.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, InflightActivationStatus::Failure);
}

#[tokio::test]
async fn test_handle_processing_deadline_discard_after() {
    let store = create_test_store().await;

    let mut batch = make_activations(2);
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
    batch[1].activation.retry_state = Some(RetryState {
        attempts: 0,
        max_attempts: 1,
        on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
        at_most_once: None,
    });

    assert!(store.store(batch).await.is_ok());

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);
}

#[tokio::test]
async fn test_handle_processing_deadline_deadletter_after() {
    let store = create_test_store().await;

    let mut batch = make_activations(2);
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
    batch[1].activation.retry_state = Some(RetryState {
        attempts: 0,
        max_attempts: 1,
        on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
        at_most_once: None,
    });

    assert!(store.store(batch).await.is_ok());

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);
    assert_count_by_status(&store, InflightActivationStatus::Processing, 0).await;
}

#[tokio::test]
async fn test_handle_processing_deadline_no_retries_remaining() {
    let store = create_test_store().await;

    let mut batch = make_activations(2);
    batch[1].status = InflightActivationStatus::Processing;
    batch[1].processing_deadline = Some(Utc.with_ymd_and_hms(2024, 11, 14, 21, 22, 23).unwrap());
    batch[1].activation.retry_state = Some(RetryState {
        attempts: 1,
        max_attempts: 1,
        on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
        at_most_once: None,
    });

    assert!(store.store(batch).await.is_ok());

    let count = store.handle_processing_deadline().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 1);
    assert_count_by_status(&store, InflightActivationStatus::Processing, 0).await;
}

#[tokio::test]
async fn test_processing_attempts_exceeded() {
    let config = create_integration_config();
    let store = create_test_store().await;

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

    let count = store.handle_processing_attempts().await;
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), 2);

    assert_count_by_status(&store, InflightActivationStatus::Processing, 0).await;
    assert_count_by_status(&store, InflightActivationStatus::Complete, 1).await;
    assert_count_by_status(&store, InflightActivationStatus::Failure, 2).await;
}

#[tokio::test]
async fn test_remove_completed() {
    let store = create_test_store().await;

    let mut records = make_activations(3);
    records[0].status = InflightActivationStatus::Complete;
    records[1].status = InflightActivationStatus::Pending;
    records[1].added_at += Duration::from_secs(1);
    records[2].status = InflightActivationStatus::Complete;
    records[2].added_at += Duration::from_secs(2);

    assert!(store.store(records.clone()).await.is_ok());

    let result = store.remove_completed().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);
    assert!(
        store
            .get_by_id(&records[0].activation.id)
            .await
            .expect("no error")
            .is_none()
    );
    assert!(
        store
            .get_by_id(&records[1].activation.id)
            .await
            .expect("no error")
            .is_some()
    );
    assert!(
        store
            .get_by_id(&records[2].activation.id)
            .await
            .expect("no error")
            .is_none()
    );

    assert_count_by_status(&store, InflightActivationStatus::Complete, 0).await;
    assert_count_by_status(&store, InflightActivationStatus::Pending, 1).await;
}

#[tokio::test]
async fn test_remove_completed_multiple_gaps() {
    let store = create_test_store().await;

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

    let result = store.remove_completed().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);
    assert!(
        store
            .get_by_id(&records[0].activation.id)
            .await
            .expect("no error")
            .is_none()
    );
    assert!(
        store
            .get_by_id(&records[1].activation.id)
            .await
            .expect("no error")
            .is_some()
    );
    assert!(
        store
            .get_by_id(&records[2].activation.id)
            .await
            .expect("no error")
            .is_none()
    );
    assert!(
        store
            .get_by_id(&records[3].activation.id)
            .await
            .expect("no error")
            .is_some()
    );
}

#[tokio::test]
async fn test_handle_failed_tasks() {
    let store = create_test_store().await;

    let mut records = make_activations(4);
    // deadletter
    records[0].status = InflightActivationStatus::Failure;
    records[0].activation.retry_state = Some(RetryState {
        attempts: 1,
        max_attempts: 1,
        on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
        at_most_once: None,
    });
    // discard
    records[1].status = InflightActivationStatus::Failure;
    records[1].activation.retry_state = Some(RetryState {
        attempts: 1,
        max_attempts: 1,
        on_attempts_exceeded: OnAttemptsExceeded::Discard as i32,
        at_most_once: None,
    });
    // no retry state = discard
    records[2].status = InflightActivationStatus::Failure;
    assert!(records[2].activation.retry_state.is_none());

    // Another deadletter
    records[3].status = InflightActivationStatus::Failure;
    records[3].activation.retry_state = Some(RetryState {
        attempts: 1,
        max_attempts: 1,
        on_attempts_exceeded: OnAttemptsExceeded::Deadletter as i32,
        at_most_once: None,
    });
    assert!(store.store(records.clone()).await.is_ok());

    let result = store.handle_failed_tasks().await;
    assert!(result.is_ok(), "handle_failed_tasks should be ok");
    let fowarder = result.unwrap();

    assert_eq!(
        fowarder.to_deadletter.len(),
        2,
        "should have two tasks to deadletter"
    );
    assert!(
        store.get_by_id(&fowarder.to_deadletter[0].id).await.is_ok(),
        "deadletter records still in sqlite"
    );
    assert!(
        store.get_by_id(&fowarder.to_deadletter[1].id).await.is_ok(),
        "deadletter records still in sqlite"
    );
    assert_eq!(fowarder.to_deadletter[0].id, records[0].activation.id);
    assert_eq!(fowarder.to_deadletter[1].id, records[3].activation.id);

    assert_count_by_status(&store, InflightActivationStatus::Failure, 2).await;
}

#[tokio::test]
async fn test_mark_completed() {
    let store = create_test_store().await;

    let records = make_activations(3);
    assert!(store.store(records.clone()).await.is_ok());
    assert_count_by_status(&store, InflightActivationStatus::Pending, 3).await;

    let ids: Vec<String> = records
        .iter()
        .map(|item| item.activation.id.clone())
        .collect();
    let result = store.mark_completed(ids.clone()).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3, "three records updated");

    // No pending tasks left
    assert_count_by_status(&store, InflightActivationStatus::Pending, 0).await;

    // All tasks should be complete
    assert_count_by_status(&store, InflightActivationStatus::Complete, 3).await;
}

#[tokio::test]
async fn test_handle_expires_at() {
    let store = create_test_store().await;
    let mut batch = make_activations(3);

    // All expired tasks should be removed, regardless of order or other tasks.
    batch[0].expires_at = Some(Utc::now() - (Duration::from_secs(5 * 60)));
    batch[1].expires_at = Some(Utc::now() + (Duration::from_secs(5 * 60)));
    batch[2].expires_at = Some(Utc::now() - (Duration::from_secs(5 * 60)));

    assert!(store.store(batch.clone()).await.is_ok());
    let result = store.handle_expires_at().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);

    assert_count_by_status(&store, InflightActivationStatus::Pending, 1).await;
    assert_count_by_status(&store, InflightActivationStatus::Failure, 0).await;
}

#[tokio::test]
async fn test_clear() {
    let store = create_test_store().await;

    #[allow(deprecated)]
    let batch = vec![InflightActivation {
        activation: TaskActivation {
            id: "id_0".into(),
            namespace: "namespace".into(),
            taskname: "taskname".into(),
            parameters: "{}".into(),
            headers: HashMap::new(),
            received_at: Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            retry_state: None,
            processing_deadline_duration: 0,
            expires: Some(1),
            delay: None,
        },
        status: InflightActivationStatus::Pending,
        partition: 0,
        offset: 0,
        added_at: Utc::now(),
        processing_attempts: 0,
        expires_at: None,
        delay_until: None,
        processing_deadline: None,
        at_most_once: false,
        namespace: "namespace".into(),
    }];
    assert!(store.store(batch).await.is_ok());
    assert_eq!(store.count().await.unwrap(), 1);

    assert!(store.clear().await.is_ok());

    assert_eq!(store.count().await.unwrap(), 0);
}

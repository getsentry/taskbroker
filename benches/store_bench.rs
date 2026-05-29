use std::sync::Arc;

use chrono::Utc;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use taskbroker::config::store::StoreConfig;
use tokio::task::JoinSet;

use taskbroker::store::activation::ActivationStatus;
use taskbroker::store::adapters::sqlite::SqliteStore;
use taskbroker::store::traits::ActivationStore;
use taskbroker::test_utils::{
    generate_temp_filename, generate_unique_namespace, make_activations_with_namespace,
};

async fn get_pending_activations(num_activations: u32, num_workers: u32) {
    let url = if cfg!(feature = "bench-with-mnt-disk") {
        let mut rng = rand::thread_rng();
        format!(
            "/mnt/disks/sqlite/{}-{}.sqlite",
            Utc::now(),
            rng.r#gen::<u64>()
        )
    } else {
        generate_temp_filename()
    };

    let mut config = StoreConfig::default();

    config.processing_deadline_grace_sec = 3;
    config.claim_lease_ms = 5000;

    config.sqlite.path = url;
    config.sqlite.vacuum_page_count = None;
    config.sqlite.enable_status_metrics = false;

    let store = Arc::new(SqliteStore::new(config).await.unwrap());

    let namespace = generate_unique_namespace();

    for chunk in make_activations_with_namespace(namespace.clone(), num_activations).chunks(1024) {
        store.store(chunk.to_vec()).await.unwrap();
    }

    assert_eq!(
        store.count_pending_activations().await.unwrap(),
        num_activations as usize
    );

    let mut join_set = JoinSet::new();
    for _ in 0..num_workers {
        let store = store.clone();
        let ns = namespace.clone();

        join_set.spawn(async move {
            let mut num_activations_processed = 0;

            while store
                .claim_activation_for_pull(Some("sentry"), Some(&ns))
                .await
                .unwrap()
                .is_some()
            {
                num_activations_processed += 1;
            }
            num_activations_processed
        });
    }
    assert_eq!(
        join_set.join_all().await.iter().sum::<u32>(),
        num_activations
    );
}

async fn set_status(num_activations: u32, num_workers: u32) {
    assert!(num_activations.is_multiple_of(num_workers));

    let url = if cfg!(feature = "bench-with-mnt-disk") {
        let mut rng = rand::thread_rng();
        format!(
            "/mnt/disks/sqlite/{}-{}.sqlite",
            Utc::now(),
            rng.r#gen::<u64>()
        )
    } else {
        generate_temp_filename()
    };

    let mut config = StoreConfig::default();

    config.processing_deadline_grace_sec = 3;
    config.claim_lease_ms = 5000;

    config.sqlite.path = url;
    config.sqlite.vacuum_page_count = None;
    config.sqlite.enable_status_metrics = false;

    let store = Arc::new(SqliteStore::new(config).await.unwrap());

    let namespace = generate_unique_namespace();

    for chunk in make_activations_with_namespace(namespace, num_activations).chunks(1024) {
        store.store(chunk.to_vec()).await.unwrap();
    }

    assert_eq!(
        store.count_pending_activations().await.unwrap(),
        num_activations as usize
    );

    let mut join_set = JoinSet::new();
    for worker_idx in 0..num_workers {
        let store = store.clone();
        join_set.spawn(async move {
            for task_id in 0..num_activations {
                if task_id % num_workers == worker_idx {
                    store
                        .set_status(
                            &format!("id_{task_id}"),
                            ActivationStatus::Complete,
                            None,
                            None,
                        )
                        .await
                        .unwrap();
                }
            }
        });
    }

    join_set.join_all().await;

    assert_eq!(
        store
            .count_by_status(ActivationStatus::Complete)
            .await
            .unwrap(),
        num_activations as usize
    );
}

fn store_bench(c: &mut Criterion) {
    let num_activations: u32 = 4_096;
    let num_workers = 64;

    c.benchmark_group("bench_ActivationStore")
        .sample_size(256)
        .throughput(criterion::Throughput::Elements(num_activations.into()))
        .bench_function("get_pending_activation", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| get_pending_activations(num_activations, num_workers));
        })
        .bench_function("set_status", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| set_status(num_activations, num_workers));
        });
}

criterion_group!(benches, store_bench);
criterion_main!(benches);

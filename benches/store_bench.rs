use std::sync::Arc;

use chrono::Utc;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use taskbroker::{
    store::inflight_activation::{
        InflightActivationStatus, InflightActivationStore, InflightActivationStoreConfig,
    },
    test_utils::{generate_temp_path, make_activations},
};
use tokio::task::JoinSet;

async fn get_pending_activations(num_activations: u32, num_workers: u32, shards: u8) {
    let url = if cfg!(feature = "bench-with-mnt-disk") {
        let mut rng = rand::thread_rng();
        format!(
            "/mnt/disks/sqlite/{}-{}.sqlite",
            Utc::now(),
            rng.r#gen::<u64>()
        )
    } else {
        generate_temp_path()
    };
    let store = Arc::new(
        InflightActivationStore::new(
            &url,
            InflightActivationStoreConfig {
                sharding_factor: shards,
                vacuum_interval_ms: 60000,
                max_processing_attempts: 1,
            },
        )
        .await
        .unwrap(),
    );

    for chunk in make_activations(num_activations).chunks(1024) {
        store.store(chunk.to_vec()).await.unwrap();
    }

    assert_eq!(
        store.count_pending_activations().await.unwrap(),
        num_activations as usize
    );

    let mut join_set = JoinSet::new();
    for _ in 0..num_workers {
        let store = store.clone();
        join_set.spawn(async move {
            let mut num_activations_processed = 0;

            while store
                .get_pending_activation(Some("namespace"))
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

async fn set_status(num_activations: u32, num_workers: u32, shards: u8) {
    assert!(num_activations % num_workers == 0);

    let url = if cfg!(feature = "bench-with-mnt-disk") {
        let mut rng = rand::thread_rng();
        format!(
            "/mnt/disks/sqlite/{}-{}.sqlite",
            Utc::now(),
            rng.r#gen::<u64>()
        )
    } else {
        generate_temp_path()
    };
    let store = Arc::new(
        InflightActivationStore::new(
            &url,
            InflightActivationStoreConfig {
                sharding_factor: shards,
                max_processing_attempts: 1,
                vacuum_interval_ms: 60000,
            },
        )
        .await
        .unwrap(),
    );

    for chunk in make_activations(num_activations).chunks(1024) {
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
                            &format!("id_{}", task_id),
                            InflightActivationStatus::Complete,
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
            .count_by_status(InflightActivationStatus::Complete)
            .await
            .unwrap(),
        num_activations as usize
    );
}

fn store_bench(c: &mut Criterion) {
    let num_activations: u32 = 4_096;
    let num_workers = 64;

    c.benchmark_group("bench_InflightActivationStore_2_shards")
        .sample_size(256)
        .throughput(criterion::Throughput::Elements(num_activations.into()))
        .bench_function("get_pending_activation", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| get_pending_activations(num_activations, num_workers, 2));
        })
        .bench_function("set_status", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| set_status(num_activations, num_workers, 2));
        });

    c.benchmark_group("bench_InflightActivationStore_4_shards")
        .sample_size(256)
        .throughput(criterion::Throughput::Elements(num_activations.into()))
        .bench_function("get_pending_activation", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| get_pending_activations(num_activations, num_workers, 4));
        })
        .bench_function("set_status", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| set_status(num_activations, num_workers, 4));
        });

    c.benchmark_group("bench_InflightActivationStore_8_shards")
        .sample_size(256)
        .throughput(criterion::Throughput::Elements(num_activations.into()))
        .bench_function("get_pending_activation", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| get_pending_activations(num_activations, num_workers, 8));
        })
        .bench_function("set_status", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| set_status(num_activations, num_workers, 8));
        });

    c.benchmark_group("bench_InflightActivationStore_16_shards")
        .sample_size(256)
        .throughput(criterion::Throughput::Elements(num_activations.into()))
        .bench_function("get_pending_activation", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| get_pending_activations(num_activations, num_workers, 16));
        })
        .bench_function("set_status", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| set_status(num_activations, num_workers, 16));
        });
}

criterion_group!(benches, store_bench);
criterion_main!(benches);

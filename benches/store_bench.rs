use std::sync::Arc;

use chrono::Utc;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use taskbroker::{
    store::inflight_activation::{
        InflightActivationStatus, InflightActivationStore, InflightActivationStoreConfig,
    },
    test_utils::{generate_temp_filename, make_activations},
};
use tokio::task::JoinSet;

async fn process_activations(num_activations: u32, num_workers: u32) {
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
    let store = Arc::new(
        InflightActivationStore::new(
            &url,
            InflightActivationStoreConfig {
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

            let mut pending = store
                .get_pending_activation(Some("namespace"))
                .await
                .unwrap();

            while let Some(ref activation) = pending {
                store
                    .set_status(
                        &activation.activation.id,
                        InflightActivationStatus::Complete,
                    )
                    .await
                    .unwrap();
                pending = store
                    .get_pending_activation(Some("namespace"))
                    .await
                    .unwrap();
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

fn store_bench(c: &mut Criterion) {
    let num_activations: u32 = 4_096;
    let num_workers = 64;

    c.benchmark_group("bench_InflightActivationStore")
        .sample_size(256)
        .throughput(criterion::Throughput::Elements(num_activations.into()))
        .bench_function("process_activations", |b| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            b.to_async(runtime)
                .iter(|| process_activations(num_activations, num_workers));
        });
}

criterion_group!(benches, store_bench);
criterion_main!(benches);

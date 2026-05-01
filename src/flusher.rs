use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::Receiver;

/// Run flusher that receives values of type T from a channel and flushes
/// them using the provided async `flush` function either when the batch is
/// full or when the max flush interval has elapsed.
pub async fn run_flusher<T, F>(
    mut rx: Receiver<T>,
    batch_size: usize,
    interval_ms: u64,
    mut flush: F,
) -> Result<()>
where
    F: for<'a> FnMut(&'a mut Vec<T>) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
{
    let batch_size = batch_size.max(1);
    let interval_ms = interval_ms.max(1);

    let period = Duration::from_millis(interval_ms);
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut buffer: Vec<T> = Vec::with_capacity(batch_size);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(v) => {
                        buffer.push(v);

                        while let Ok(update) = rx.try_recv() {
                            buffer.push(update);
                        }

                        if buffer.len() >= batch_size {
                            flush(&mut buffer).await;
                        }
                    }

                    None => {
                        // Channel closed (shutdown), flush remaining and exit
                        flush(&mut buffer).await;
                        break;
                    }
                }
            }

            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush(&mut buffer).await;
                }
            }
        }
    }

    Ok(())
}

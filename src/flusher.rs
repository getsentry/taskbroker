use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::Receiver;

/// Run flusher that receives values of type T from a channel and flushes
/// them using the provided async `flush` function either when the batch is
/// full or when the max flush interval has elapsed. This function is **not**
/// responsible for draining the buffer - `flush` does that.
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
            // When the buffer is NOT full, try to receive another message
            msg = rx.recv(), if buffer.len() < batch_size => {
                match msg {
                    Some(v) => {
                        buffer.push(v);

                        while buffer.len() < batch_size && let Ok(update) = rx.try_recv() {
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

            // If the buffer IS full, the branch above will never execute, and we will never
            // discover that the channel is now closed, which is why this branch is necessary
            _ = std::future::ready(()), if rx.is_closed() => {
                while let Ok(update) = rx.try_recv() {
                    // Buffer may grow beyond configured limit, which is OK because we are shutting down
                    buffer.push(update);
                }

                flush(&mut buffer).await;
                break;
            }

            // Otherwise, try flushing whatever is in the buffer every `interval_ms` milliseconds
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush(&mut buffer).await;
                }
            }
        }
    }

    Ok(())
}

use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use metrics_exporter_statsd::{StatsdBuilder, StatsdRecorder};
use metrics_util::MetricKindMask;
use metrics_util::layers::{Layer, PrefixLayer, Router, RouterBuilder};

use crate::config::Config;

pub struct MetricsConfig {
    pub statsd_addr: SocketAddr,
    pub default_tags: BTreeMap<String, String>,
}

/// Library metric namespaces that bypass the Taskbroker prefix.
const CUSTOM_NAMESPACES: &[&str] = &["arroyo."];

fn route_recorder(recorder: Arc<StatsdRecorder>) -> Router {
    let prefixed = PrefixLayer::new("taskbroker").layer(Arc::clone(&recorder));
    let mut router = RouterBuilder::from_recorder(prefixed);
    for namespace in CUSTOM_NAMESPACES {
        router.add_route(MetricKindMask::ALL, namespace, Arc::clone(&recorder));
    }
    router.build()
}

impl MetricsConfig {
    pub fn from_config(config: &Config) -> Self {
        let socket_addrs = config
            .statsd_addr
            .to_socket_addrs()
            .expect("Could not resolve into a socket address");
        let [statsd_addr] = socket_addrs.as_slice() else {
            unreachable!("Expect statsd_addr to resolve into a single socket address");
        };
        MetricsConfig {
            statsd_addr: *statsd_addr,
            default_tags: config.default_metrics_tags.clone(),
        }
    }
}

pub fn init(metrics_config: MetricsConfig) {
    let address = metrics_config.statsd_addr;

    let builder = StatsdBuilder::from(address.ip().to_string(), address.port());

    let recorder = Arc::new(
        metrics_config
            .default_tags
            .into_iter()
            .fold(
                builder.with_queue_size(5000).with_buffer_size(1024),
                |builder, (key, value)| builder.with_default_tag(key, value),
            )
            .build(None)
            .expect("Could not create StatsdRecorder"),
    );

    metrics::set_global_recorder(route_recorder(recorder))
        .expect("Could not set global metrics recorder")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use cadence::SpyMetricSink;
    use metrics_exporter_statsd::StatsdBuilder;

    use super::route_recorder;

    #[test]
    fn test_metrics_prefix() {
        let (emitted, sink) = SpyMetricSink::new();
        let statsd = Arc::new(
            StatsdBuilder::from("unused", 1)
                .with_sink(sink)
                .build(None)
                .unwrap(),
        );
        let recorder = route_recorder(statsd);

        metrics::with_local_recorder(&recorder, || {
            metrics::gauge!("arroyo.consumer.current_partitions", "application" => "taskbroker")
                .set(1.0);
            metrics::counter!("upkeep.retries").increment(1);
        });

        let expected = BTreeSet::from([
            "arroyo.consumer.current_partitions:1|g|#application:taskbroker".to_owned(),
            "taskbroker.upkeep.retries:1|c".to_owned(),
        ]);
        let actual = (0..expected.len())
            .map(|_| {
                String::from_utf8(emitted.recv_timeout(Duration::from_secs(1)).unwrap()).unwrap()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
    }
}

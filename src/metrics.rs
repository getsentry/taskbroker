use crate::config::Config;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use metrics_exporter_statsd::StatsdBuilder;
use std::{
    collections::BTreeMap,
    net::{SocketAddr, ToSocketAddrs},
};

pub struct MetricsConfig {
    pub metrics_host: String,
    pub statsd_addr: SocketAddr,
    pub default_tags: BTreeMap<String, String>,
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
            metrics_host: config.metrics_host.clone(),
            default_tags: config.default_metrics_tags.clone(),
        }
    }
}

pub fn init(metrics_config: MetricsConfig) {
    if metrics_config.metrics_host.is_empty() {
        // Fallback to statsd forwarder
        let address = metrics_config.statsd_addr;

        let builder = StatsdBuilder::from(address.ip().to_string(), address.port());

        let recorder = metrics_config
            .default_tags
            .into_iter()
            .fold(
                builder.with_queue_size(5000).with_buffer_size(1024),
                |builder, (key, value)| builder.with_default_tag(key, value),
            )
            .build(Some("taskbroker"))
            .expect("Could not create StatsdRecorder");

        metrics::set_global_recorder(recorder).expect("Could not set global metrics recorder")
    } else {
        let default_tags = metrics_config
            .default_tags
            .into_iter()
            .map(|(key, value)| metrics::Label::new(key, value))
            .collect();

        // Use dogstatsd exporter if enabled.
        let builder = DogStatsDBuilder::default()
            .with_remote_address(metrics_config.metrics_host)
            .expect("Could not set metrics host address")
            .with_telemetry(true)
            .send_histograms_as_distributions(true)
            .with_histogram_sampling(true)
            .set_global_prefix("taskbroker")
            .with_global_labels(default_tags);

        builder
            .install()
            .expect("Could not create DogStatsDBuilder");
    }
}

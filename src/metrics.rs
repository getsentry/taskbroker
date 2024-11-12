use std::net::SocketAddr;
use metrics_exporter_statsd::StatsdBuilder;
use crate::config::Config;

pub struct MetricsConfig {
    pub statsd_addr: SocketAddr,
}

impl MetricsConfig {
    pub fn from_config(config: &Config) -> Self {
        MetricsConfig {
            statsd_addr: config.statsd_addr,
        }
    }
}

pub fn init(metrics_config: MetricsConfig) {
    let address = metrics_config.statsd_addr;

    let recorder = StatsdBuilder::from(address.ip().to_string(), address.port())
        .with_queue_size(5000)
        .with_buffer_size(1024)
        .build(Some("taskbroker"))
        .expect("Could not create StatsdRecorder");

    metrics::set_global_recorder(recorder).expect("Could not set global metrics recorder")
}

use crate::config::Config;
use metrics_exporter_statsd::StatsdBuilder;
use std::{
    collections::BTreeMap,
    net::{SocketAddr, ToSocketAddrs},
};

pub struct MetricsConfig {
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
            default_tags: config.default_metrics_tags.clone(),
        }
    }
}

pub fn init(metrics_config: MetricsConfig) {
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
}

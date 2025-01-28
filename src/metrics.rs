use crate::config::Config;
use metrics_exporter_dogstatsd::DogStatsDBuilder;

pub struct MetricsConfig {
    pub dog_statsd_addr: Option<String>,
}

impl MetricsConfig {
    pub fn from_config(config: &Config) -> Self {
        MetricsConfig {
            dog_statsd_addr: config.dog_statsd_addr.clone(),
        }
    }
}

pub fn init(metrics_config: MetricsConfig) {
    let Some(dog_statd_addr) = metrics_config.dog_statsd_addr else {
        return;
    };
    DogStatsDBuilder::default()
        .with_remote_address(dog_statd_addr)
        .expect("Failed to parse remote address")
        .with_telemetry(false)
        .install()
        .expect("Failed to install DogStatsD recorder");
}

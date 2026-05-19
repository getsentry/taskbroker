use serde::{Deserialize, Serialize};

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct UpkeepConfig {
    /// Number of milliseconds between upkeep runs.
    pub upkeep_interval_ms: u64,

    /// The number of milliseconds between upkeep runs that indicates unhealthy
    /// performance that should trigger a restart.
    pub upkeep_unhealthy_interval_ms: u64,

    /// The number of seconds that deadline resets are skipped after startup.
    /// This delay allows workers time to publish results after a broker restart.
    pub upkeep_deadline_reset_skip_after_startup_sec: u64,

    /// Whether to skip the health check if the pods are in a bad state.
    pub health_check_killswitched: bool,

    /// Maximum number of times a task can be reset from processing back to pending.
    /// When this limit is reached, the activation will be discarded or deadlettered.
    pub max_processing_attempts: usize,

    /// Enable the upkeep thread to perforam a full `VACUUM` on the database periodically.
    pub full_vacuum_on_upkeep: bool,

    /// The interval in milliseconds between full `VACUUM`s on the database by the upkeep thread.
    pub vacuum_interval_ms: u64,
}

impl Default for UpkeepConfig {
    fn default() -> Self {
        Self {
            upkeep_interval_ms: 1000,
            upkeep_unhealthy_interval_ms: 5000,
            upkeep_deadline_reset_skip_after_startup_sec: 60,
            health_check_killswitched: false,
            max_processing_attempts: 5,
            full_vacuum_on_upkeep: true,
            vacuum_interval_ms: 30000,
        }
    }
}

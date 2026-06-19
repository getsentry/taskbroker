use serde::{Deserialize, Serialize};

/// Raw mode settings for a topic.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RawModeConfig {
    /// The namespace to assign to raw mode activations.
    pub namespace: Option<String>,

    /// The application to assign to raw mode activations.
    pub application: Option<String>,

    /// The taskname to assign to raw mode activations.
    pub taskname: Option<String>,

    /// Processing deadline duration in seconds for raw mode activations.
    pub processing_deadline_duration: Option<u16>,

    /// zstd compression level for raw-mode payloads. Defaults to 3 (matching the
    /// producer's zstandard default). Set to -1 to disable compression.
    #[serde(default)]
    pub compression_level: Option<i32>,
}

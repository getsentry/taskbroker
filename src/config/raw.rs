use serde::{Deserialize, Serialize};

/// Raw mode configuration.
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct RawConfig {
    /// The namespace to assign to raw mode activations.
    pub namespace: String,

    /// The application to assign to raw mode activations.
    pub application: String,

    /// The taskname to assign to raw mode activations.
    pub taskname: String,

    /// Processing deadline duration in seconds for raw mode activations. This is
    /// a `u16` because (1) we don't want to allow signed numbers, and (2) it
    /// can be cast into `i32` (which we use elsewhere) without error conditions
    pub processing_deadline_duration: u16,
}

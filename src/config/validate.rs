use std::time::Duration;
use validator::ValidationError;

/// Ensure `n` is a power of two. Used to validate fetch thread count.
pub fn power_of_two(n: usize) -> Result<(), ValidationError> {
    if n.is_power_of_two() {
        Ok(())
    } else {
        Err(ValidationError::new("not_power_of_two"))
    }
}

/// Ensure duration is greater than zero to avoid crashes when initializing intervals.
pub fn nonzero_duration(duration: &Duration) -> Result<(), ValidationError> {
    if duration.is_zero() {
        Err(ValidationError::new("nonzero_duration"))
    } else {
        Ok(())
    }
}

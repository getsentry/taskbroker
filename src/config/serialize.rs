use std::time::Duration;

use serde::ser::Error;

/// Serialize a `Duration` as a millisecond duration string.
pub fn duration<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if !duration.subsec_nanos().is_multiple_of(1_000_000) {
        let msg = "Duration must be representable in milliseconds";
        return Err(S::Error::custom(msg));
    }

    serializer.serialize_str(&format!("{}ms", duration.as_millis()))
}

#[cfg(test)]
mod tests {
    use serde::Serialize;
    use std::time::Duration;

    #[derive(Debug, Serialize)]
    struct DurationMock {
        #[serde(serialize_with = "super::duration")]
        duration: Duration,
    }

    #[test]
    fn test_duration_valid_examples() {
        let examples = [
            (Duration::from_millis(0), "duration: 0ms\n"),
            (Duration::from_millis(1), "duration: 1ms\n"),
            (Duration::from_millis(250), "duration: 250ms\n"),
            (Duration::from_secs(1), "duration: 1000ms\n"),
            (Duration::from_secs(42), "duration: 42000ms\n"),
        ];

        for (duration, expected) in examples {
            let serialized = serde_yaml::to_string(&DurationMock { duration }).unwrap();
            assert_eq!(serialized, expected);
        }
    }

    #[test]
    fn test_duration_invalid_examples() {
        let examples = [
            Duration::from_nanos(1),
            Duration::from_micros(1),
            Duration::from_millis(1) + Duration::from_nanos(1),
            Duration::from_secs(1) + Duration::from_micros(1),
        ];

        for duration in examples {
            let result = serde_yaml::to_string(&DurationMock { duration });
            assert!(
                result.is_err(),
                "Invalid duration '{duration:?}' should fail to serialize"
            );
        }
    }
}

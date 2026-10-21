pub mod duration {
    use std::time::Duration;

    use ::serde::Deserialize;
    use ::serde::de::Error as DeError;
    use ::serde::ser::Error as SerError;

    /// Serialize a `Duration` as a millisecond duration string.
    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        if !duration.subsec_nanos().is_multiple_of(1_000_000) {
            let msg = "Duration must be representable in milliseconds";
            return Err(S::Error::custom(msg));
        }

        serializer.serialize_str(&format!("{}ms", duration.as_millis()))
    }

    /// Deserialize a string duration with an `ms` or `s` suffix.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;

        if let Some(millis) = value.strip_suffix("ms") {
            let millis = millis.parse::<u64>().map_err(D::Error::custom)?;
            return Ok(Duration::from_millis(millis));
        }

        if let Some(seconds) = value.strip_suffix("s") {
            let seconds = seconds.parse::<u64>().map_err(D::Error::custom)?;
            return Ok(Duration::from_secs(seconds));
        }

        let msg = "Expected an integer followed immediately by either 'ms' or 's'";
        Err(D::Error::custom(msg))
    }

    #[cfg(test)]
    mod tests {
        use std::time::Duration;

        use ::serde::{Deserialize, Serialize};

        #[derive(Debug, Deserialize, Serialize)]
        struct DurationMock {
            #[serde(with = "super")]
            duration: Duration,
        }

        #[test]
        fn test_duration_deserialize_valid_examples() {
            let examples = [
                ("0ms", Duration::from_millis(0)),
                ("1ms", Duration::from_millis(1)),
                ("250ms", Duration::from_millis(250)),
                ("0s", Duration::from_secs(0)),
                ("1s", Duration::from_secs(1)),
                ("42s", Duration::from_secs(42)),
            ];

            for (value, expected) in examples {
                let parsed: DurationMock =
                    serde_yaml::from_str(&format!("duration: {value}")).unwrap();
                assert_eq!(parsed.duration, expected);
            }
        }

        #[test]
        fn test_duration_deserialize_invalid_examples() {
            let examples = [
                "", "1", "1m", "1 ms", "ms", "s", "-1ms", "1.5s", "1MS", "1h",
            ];

            for value in examples {
                let result = serde_yaml::from_str::<DurationMock>(&format!("duration: {value:?}"));
                assert!(
                    result.is_err(),
                    "Invalid duration '{value:?}' should fail to deserialize"
                );
            }
        }

        #[test]
        fn test_duration_serialize_valid_examples() {
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
        fn test_duration_serialize_invalid_examples() {
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
}

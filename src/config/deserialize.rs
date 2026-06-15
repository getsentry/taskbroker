use std::time::Duration;

use serde::Deserialize;
use serde::de::Error;

/// Deserialize a string duration with an `ms` or `s` suffix.
pub fn duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
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
    use serde::Deserialize;
    use std::time::Duration;

    #[derive(Debug, Deserialize)]
    struct DurationMock {
        #[serde(deserialize_with = "super::duration")]
        duration: Duration,
    }

    #[test]
    fn test_duration_valid_examples() {
        let examples = [
            ("0ms", Duration::from_millis(0)),
            ("1ms", Duration::from_millis(1)),
            ("250ms", Duration::from_millis(250)),
            ("0s", Duration::from_secs(0)),
            ("1s", Duration::from_secs(1)),
            ("42s", Duration::from_secs(42)),
        ];

        for (value, expected) in examples {
            let parsed: DurationMock = serde_yaml::from_str(&format!("duration: {value}")).unwrap();
            assert_eq!(parsed.duration, expected);
        }
    }

    #[test]
    fn test_duration_invalid_examples() {
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
}

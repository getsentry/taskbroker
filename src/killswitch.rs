//! Fine-grained task killswitches.
//!
//! The `drop_task_killswitch` runtime config is a list of rules. A rule is either:
//!
//! * a bare task name (`KillswitchRule::Name`) — drop every activation of that task, or
//! * a selector (`KillswitchRule::Selector`) — drop an activation only when its arguments
//!   match, so we can e.g. drop `sentry.tasks.unmerge` for a single `project_id`.
//!
//! Selectors match against the activation's `parameters_bytes`, which is msgpack-encoded
//! as `{"args": [...], "kwargs": {...}}` and may be zstd-compressed (see
//! `headers["compression-type"]`). All parsing **fails open**: any decode error means the
//! task is *not* dropped, so a killswitch can never take down traffic it wasn't meant to.

use std::collections::HashMap;

use sentry_protos::taskbroker::v1::TaskActivation;
use serde::Deserialize;

use crate::kafka::deserialize_raw::{COMPRESSION_HEADER, COMPRESSION_ZSTD};

/// Upper bound for decompressing `parameters_bytes`, mirroring the worker's limit in
/// `deserialize_raw`.
const MAX_DECOMPRESSED_SIZE: usize = 64 * 1024 * 1024;

/// A single entry in `drop_task_killswitch`.
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum KillswitchRule {
    /// A bare task name: drop every activation of this task.
    Name(String),
    /// An argument selector: drop only activations whose arguments match.
    Selector(KillswitchSelector),
}

/// Drop activations of `name` whose arguments match any of the listed conditions.
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct KillswitchSelector {
    /// The task name this selector applies to.
    pub name: String,
    /// Match by positional argument index, e.g. `{0: 123}` for "first arg == 123".
    #[serde(default)]
    pub select_arg: HashMap<usize, serde_yaml::Value>,
    /// Match by keyword argument name, e.g. `{project_id: 123}`.
    #[serde(default)]
    pub select_kwarg: HashMap<String, serde_yaml::Value>,
}

impl KillswitchRule {
    /// The task name this rule applies to, regardless of variant.
    pub fn taskname(&self) -> &str {
        match self {
            KillswitchRule::Name(name) => name,
            KillswitchRule::Selector(selector) => &selector.name,
        }
    }
}

/// Split a rule list into the whole-task names (handled by a fast `taskname IN (...)` SQL
/// path) and the argument selectors (which require decoding each activation).
pub fn partition_rules(rules: &[KillswitchRule]) -> (Vec<String>, Vec<KillswitchSelector>) {
    let mut names = Vec::new();
    let mut selectors = Vec::new();
    for rule in rules {
        match rule {
            KillswitchRule::Name(name) => names.push(name.clone()),
            KillswitchRule::Selector(selector) => selectors.push(selector.clone()),
        }
    }
    (names, selectors)
}

/// Decoded `{"args": [...], "kwargs": {...}}` from an activation's `parameters_bytes`.
#[derive(Debug, Deserialize)]
struct DecodedParams {
    #[serde(default)]
    args: Vec<serde_yaml::Value>,
    #[serde(default)]
    kwargs: HashMap<String, serde_yaml::Value>,
}

/// Decode (and zstd-decompress if needed) an activation's `parameters_bytes`. Returns
/// `None` on any error so callers fail open.
fn decode_params(activation: &TaskActivation) -> Option<DecodedParams> {
    let is_zstd = activation
        .headers
        .get(COMPRESSION_HEADER)
        .map(String::as_str)
        == Some(COMPRESSION_ZSTD);

    let decompressed;
    let raw: &[u8] = if is_zstd {
        decompressed =
            zstd::bulk::decompress(&activation.parameters_bytes, MAX_DECOMPRESSED_SIZE).ok()?;
        &decompressed
    } else {
        &activation.parameters_bytes
    };

    rmp_serde::from_slice::<DecodedParams>(raw).ok()
}

/// Whether `selector` matches `activation` (so the task should be dropped).
///
/// Conditions combine with OR: a match on any listed `select_arg` or `select_kwarg` is
/// enough. Fails open — any decode error returns `false`.
pub fn selector_matches(selector: &KillswitchSelector, activation: &TaskActivation) -> bool {
    let Some(params) = decode_params(activation) else {
        return false;
    };

    // `serde_yaml::Value` equality is exact: a positive int from msgpack and the same int
    // from YAML both deserialize to the `PosInt` variant (sign decides the variant on both
    // sides), so `==` matches them. Int vs float (`123` vs `123.0`) intentionally does not.
    let arg_match = selector
        .select_arg
        .iter()
        .any(|(index, want)| params.args.get(*index) == Some(want));
    let kwarg_match = selector
        .select_kwarg
        .iter()
        .any(|(key, want)| params.kwargs.get(key) == Some(want));

    arg_match || kwarg_match
}

/// Decide whether an ingested activation should be dropped per `rules`.
///
/// Returns the metric `match` label (`"whole_task"` or `"selector"`) when the task should
/// be dropped, or `None` to keep it. Allocates nothing when no rule targets `taskname`,
/// and only decodes `activation_bytes` (the raw `TaskActivation` protobuf) when a selector
/// actually targets the task. Fails open: a protobuf decode error keeps the task.
pub fn should_drop(
    rules: &[KillswitchRule],
    taskname: &str,
    activation_bytes: &[u8],
) -> Option<&'static str> {
    use prost::Message as _;

    // Whole-task killswitch: drop every activation of this task.
    if rules
        .iter()
        .any(|rule| matches!(rule, KillswitchRule::Name(name) if name == taskname))
    {
        return Some("whole_task");
    }

    // Selector killswitch: only decode the activation when a selector targets this task.
    let has_selector = rules
        .iter()
        .any(|rule| matches!(rule, KillswitchRule::Selector(sel) if sel.name == taskname));
    if has_selector
        && let Ok(activation) = TaskActivation::decode(activation_bytes)
        && rules.iter().any(|rule| {
            matches!(rule, KillswitchRule::Selector(sel)
                if sel.name == taskname && selector_matches(sel, &activation))
        })
    {
        return Some("selector");
    }

    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde::Serialize;

    use crate::test_utils::TaskActivationBuilder;

    use super::*;

    /// Build a `TaskActivation` whose `parameters_bytes` is the msgpack encoding of `params`.
    fn activation_with_params<T: Serialize>(params: &T) -> TaskActivation {
        let bytes = rmp_serde::to_vec_named(params).unwrap();
        TaskActivationBuilder::new()
            .id("id")
            .application("sentry")
            .namespace("ns")
            .taskname("sentry.tasks.unmerge")
            .parameters_bytes(bytes)
            .build()
    }

    #[derive(Serialize)]
    struct Params {
        args: Vec<serde_yaml::Value>,
        kwargs: HashMap<String, serde_yaml::Value>,
    }

    fn yaml(value: &str) -> serde_yaml::Value {
        serde_yaml::from_str(value).unwrap()
    }

    fn selector(select_arg: &[(usize, &str)], select_kwarg: &[(&str, &str)]) -> KillswitchSelector {
        KillswitchSelector {
            name: "sentry.tasks.unmerge".into(),
            select_arg: select_arg.iter().map(|(i, v)| (*i, yaml(v))).collect(),
            select_kwarg: select_kwarg
                .iter()
                .map(|(k, v)| (k.to_string(), yaml(v)))
                .collect(),
        }
    }

    #[test]
    fn test_match_positional_arg() {
        let activation = activation_with_params(&Params {
            args: vec![yaml("123")],
            kwargs: HashMap::new(),
        });
        assert!(selector_matches(&selector(&[(0, "123")], &[]), &activation));
        assert!(!selector_matches(
            &selector(&[(0, "999")], &[]),
            &activation
        ));
        // index out of range fails open (no match)
        assert!(!selector_matches(
            &selector(&[(5, "123")], &[]),
            &activation
        ));
    }

    #[test]
    fn test_match_kwarg() {
        let activation = activation_with_params(&Params {
            args: vec![],
            kwargs: HashMap::from([("project_id".to_string(), yaml("123"))]),
        });
        assert!(selector_matches(
            &selector(&[], &[("project_id", "123")]),
            &activation
        ));
        assert!(!selector_matches(
            &selector(&[], &[("project_id", "999")]),
            &activation
        ));
        // unknown kwarg name does not match
        assert!(!selector_matches(
            &selector(&[], &[("org_id", "123")]),
            &activation
        ));
    }

    #[test]
    fn test_or_semantics() {
        // Called positionally: kwarg condition misses, arg condition hits -> drop.
        let activation = activation_with_params(&Params {
            args: vec![yaml("123")],
            kwargs: HashMap::new(),
        });
        let sel = selector(&[(0, "123")], &[("project_id", "123")]);
        assert!(selector_matches(&sel, &activation));

        // Called by keyword: arg condition misses, kwarg condition hits -> drop.
        let activation = activation_with_params(&Params {
            args: vec![],
            kwargs: HashMap::from([("project_id".to_string(), yaml("123"))]),
        });
        assert!(selector_matches(&sel, &activation));

        // Neither matches -> keep.
        let activation = activation_with_params(&Params {
            args: vec![yaml("1")],
            kwargs: HashMap::from([("project_id".to_string(), yaml("2"))]),
        });
        assert!(!selector_matches(&sel, &activation));
    }

    #[test]
    fn test_match_wide_and_negative_ints() {
        // Values that don't fit a small int marker, plus a negative, to confirm direct
        // `serde_yaml::Value` equality holds across the msgpack encoding.
        let activation = activation_with_params(&Params {
            args: vec![],
            kwargs: HashMap::from([
                ("project_id".to_string(), yaml("10000000000")),
                ("delta".to_string(), yaml("-5")),
            ]),
        });
        assert!(selector_matches(
            &selector(&[], &[("project_id", "10000000000")]),
            &activation
        ));
        assert!(selector_matches(
            &selector(&[], &[("delta", "-5")]),
            &activation
        ));
        assert!(!selector_matches(
            &selector(&[], &[("project_id", "10000000001")]),
            &activation
        ));
    }

    #[test]
    fn test_zstd_compressed() {
        let bytes = rmp_serde::to_vec_named(&Params {
            args: vec![],
            kwargs: HashMap::from([("project_id".to_string(), yaml("123"))]),
        })
        .unwrap();
        let compressed = zstd::bulk::compress(&bytes, 3).unwrap();
        let activation = TaskActivationBuilder::new()
            .id("id")
            .application("sentry")
            .namespace("ns")
            .taskname("sentry.tasks.unmerge")
            .parameters_bytes(compressed)
            .headers(HashMap::from([(
                COMPRESSION_HEADER.to_string(),
                COMPRESSION_ZSTD.to_string(),
            )]))
            .build();
        assert!(selector_matches(
            &selector(&[], &[("project_id", "123")]),
            &activation
        ));
    }

    #[test]
    fn test_fail_open_on_garbage() {
        // Not valid msgpack -> no match.
        let activation = TaskActivationBuilder::new()
            .id("id")
            .application("sentry")
            .namespace("ns")
            .taskname("sentry.tasks.unmerge")
            .parameters_bytes(vec![0xff, 0xff, 0xff])
            .build();
        assert!(!selector_matches(
            &selector(&[(0, "123")], &[("project_id", "123")]),
            &activation
        ));
    }

    #[test]
    fn test_empty_params() {
        // Empty msgpack map `{}` (the test default) -> no args/kwargs, no match.
        let activation = TaskActivationBuilder::new()
            .id("id")
            .application("sentry")
            .namespace("ns")
            .taskname("sentry.tasks.unmerge")
            .build();
        assert!(!selector_matches(
            &selector(&[(0, "123")], &[("project_id", "123")]),
            &activation
        ));
    }
}

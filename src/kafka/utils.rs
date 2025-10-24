use prost::Message;
use sentry_protos::taskbroker::v1::TaskActivation;

/// The default namespace suffix for demoted/forwarded tasks
pub const DEFAULT_DEMOTED_NAMESPACE_SUFFIX: &str = "_long";

/// Modifies a TaskActivation's namespace for forwarding to demoted topic.
/// This prevents infinite forwarding loops when tasks are received by another
/// broker with the same demoted_namespaces configuration.
///
/// # Arguments
/// * `activation_bytes` - The serialized TaskActivation protobuf
///
/// # Returns
/// * `Ok(Vec<u8>)` - The protobuf of the TaskActivation with namespace modified
/// * `Err(DecodeError)` - If the protobuf cannot be decoded
pub fn modify_activation_namespace(activation_bytes: &[u8]) -> Result<Vec<u8>, prost::DecodeError> {
    let mut activation = TaskActivation::decode(activation_bytes)?;
    activation.namespace += DEFAULT_DEMOTED_NAMESPACE_SUFFIX;
    Ok(activation.encode_to_vec())
}

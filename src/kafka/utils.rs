use prost::Message;
use sentry_protos::taskbroker::v1::TaskActivation;

/// Tags a TaskActivation for forwarding to demoted topic by adding a header marker.
/// This prevents infinite forwarding loops when tasks are received by another
/// broker with the same demoted_namespaces configuration.
///
/// # Arguments
/// * `activation_bytes` - the activation protobuf of a TaskActivation
///
/// # Returns
/// * `Ok(Some(Vec<u8>))` - The protobuf of the TaskActivation with forwarding header added
/// * `Ok(None)` - Already tagged for forwarding (skip forwarding)
/// * `Err(DecodeError)` - If the protobuf cannot be decoded
pub fn tag_for_forwarding(activation_bytes: &[u8]) -> Result<Option<Vec<u8>>, prost::DecodeError> {
    let mut activation = TaskActivation::decode(activation_bytes)?;
    if activation.headers.get("forwarded").map(|v| v.as_str()) == Some("true") {
        return Ok(None);
    }
    activation
        .headers
        .insert("forwarded".to_string(), "true".to_string());
    Ok(Some(activation.encode_to_vec()))
}

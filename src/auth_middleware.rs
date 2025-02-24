use anyhow::Context;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use hmac::{Hmac, Mac};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use sha2::Sha256;
use std::mem;
use std::task::{self, Poll};
use tower::{Layer, Service};

use crate::config::Config;

type Hmac256 = Hmac<Sha256>;

/// Tower layer to connect authentication logic with the grpc server
#[derive(Debug, Clone, Default)]
pub struct AuthLayer {
    pub shared_secret: Vec<String>,
}

impl AuthLayer {
    pub fn new(config: &Config) -> Self {
        Self {
            shared_secret: config.grpc_shared_secret.clone(),
        }
    }
}

impl<Inner> Layer<Inner> for AuthLayer {
    type Service = AuthService<Inner>;

    fn layer(&self, service: Inner) -> Self::Service {
        AuthService::new(self.shared_secret.clone(), service)
    }
}

/// Tower service that provides a home for the authentication logic
#[derive(Debug, Clone)]
pub struct AuthService<Inner> {
    secret: Vec<String>,
    inner: Inner,
}

impl<Inner> AuthService<Inner> {
    pub fn new(secret: Vec<String>, inner: Inner) -> Self {
        Self { secret, inner }
    }
}

// We need to narrow the request trait bounds so that we can read the request body
// and set the response body.
type TonicBody = UnsyncBoxBody<Bytes, tonic::Status>;

impl<Inner> Service<http::Request<TonicBody>> for AuthService<Inner>
where
    Inner: Service<http::Request<TonicBody>, Response = http::Response<TonicBody>>
        + Clone
        + Send
        + 'static,
    Inner::Future: Send,
{
    type Response = Inner::Response;
    type Error = Inner::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let secret = self.secret.clone();
        let mut inner = self.inner.clone();
        mem::swap(&mut inner, &mut self.inner);

        // Because we need to read the request body we need an async block
        // and to pin the futures.
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let body_bytes = body.collect().await.unwrap().to_bytes();

            match validate_signature(&secret, &parts, body_bytes) {
                Ok(body) => {
                    // reconstruct a request with the bytes we read from the request.
                    // tonic::body::boxed satifies the TonicBody trait bounds.
                    let new_body = Full::new(body);
                    let boxed_body = tonic::body::boxed(new_body);
                    let new_req = http::Request::from_parts(parts, boxed_body);

                    inner.call(new_req).await
                }
                Err(error) => {
                    tracing::error!("GRPC Authentication error: {error}");
                    let response =
                        tonic::Status::unauthenticated("Authentication failed").into_http();
                    Ok(response)
                }
            }
        })
    }
}

fn validate_signature(
    secret: &[String],
    req_head: &http::request::Parts,
    req_body: Bytes,
) -> anyhow::Result<Bytes> {
    if secret.is_empty() {
        return Ok(req_body);
    }

    let signature = req_head
        .headers
        .get("sentry-signature")
        .context("missing sentry-signature header")?
        .to_str()
        .unwrap_or_default();

    // Signature in the request headers is hex encoded
    let signature = hex::decode(signature).unwrap_or_default();

    // Slice off the leading nulls. Real requests
    // come with a bunch of null bytes.
    let mut index = 0;
    for byte in req_body.iter() {
        if *byte != b'\0' {
            break;
        }
        index += 1;
    }
    let req_body_trim = &req_body[index..];

    for possible_key in secret {
        let mut hmac = Hmac256::new_from_slice(possible_key.as_bytes()).unwrap();
        hmac.update(req_head.uri.path().as_bytes());
        hmac.update(b":");
        hmac.update(req_body_trim);

        if hmac.verify_slice(&signature[..]).is_ok() {
            return Ok(req_body);
        }
    }

    anyhow::bail!("Invalid request signature")
}

#[cfg(test)]
mod tests {
    use super::validate_signature;
    use bytes::Bytes;
    use hmac::{Hmac, Mac};
    use http::request::Request;
    use sha2::Sha256;

    type Hmac256 = Hmac<Sha256>;

    fn generate_hmac(secret: &str, path: &str, body: &Bytes) -> String {
        let mut hmac = Hmac256::new_from_slice(secret.as_bytes()).unwrap();
        hmac.update(path.as_bytes());
        hmac.update(b":");
        hmac.update(body);
        let output = hmac.finalize();
        hex::encode(output.into_bytes())
    }

    #[test]
    fn test_validate_signature_missing_secret() {
        let secret: Vec<String> = vec![];
        let request = Request::builder()
            .header("sentry-signature", "not real")
            .body(Bytes::from("request data"))
            .unwrap();
        let (parts, body) = request.into_parts();

        let res = validate_signature(&secret, &parts, body);
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_signature_missing_header() {
        let secret: Vec<String> = vec!["super secret".into()];
        let request = Request::builder()
            .body(Bytes::from("request data"))
            .unwrap();
        let (parts, body) = request.into_parts();

        let res = validate_signature(&secret, &parts, body);
        assert!(res.is_err());
    }

    #[test]
    fn test_validate_signature_empty_header() {
        let secret: Vec<String> = vec!["super secret".into()];
        let request = Request::builder()
            .header("sentry-signature", "")
            .body(Bytes::from("request data"))
            .unwrap();
        let (parts, body) = request.into_parts();

        let res = validate_signature(&secret, &parts, body);
        assert!(res.is_err());
    }

    #[test]
    fn test_validate_signature_incorrect_signature() {
        let secret: Vec<String> = vec!["super secret".into()];
        let request = Request::builder()
            .header("sentry-signature", "not-hex-encoded-and-invalid")
            .body(Bytes::from("request data"))
            .unwrap();
        let (parts, body) = request.into_parts();

        let res = validate_signature(&secret, &parts, body);
        assert!(res.is_err());
    }

    #[test]
    fn test_validate_signature_path_mismatch() {
        let secret: Vec<String> = vec!["super secret".into()];
        let body = Bytes::from("request data");
        let signature = generate_hmac(&secret[0], "/rpc/service/oops", &body);
        let request = Request::builder()
            .uri("http://example.org/rpc/service/method")
            .header("sentry-signature", signature)
            .body(body)
            .unwrap();
        let (parts, body) = request.into_parts();

        let res = validate_signature(&secret, &parts, body);
        assert!(res.is_err());
    }

    #[test]
    fn test_validate_signature_success() {
        let secret: Vec<String> = vec!["super secret".into()];
        let body = Bytes::from("request data");
        let signature = generate_hmac(&secret[0], "/rpc/service/method", &body);
        let request = Request::builder()
            .uri("http://example.org/rpc/service/method")
            .header("sentry-signature", signature)
            .body(body)
            .unwrap();
        let (parts, body) = request.into_parts();

        let res = validate_signature(&secret, &parts, body);
        assert!(res.is_ok());
        let result_body = res.unwrap();
        assert!(result_body == *"request data");
    }

    #[test]
    fn test_validate_signature_success_null_slicing() {
        let secret: Vec<String> = vec!["super secret".into()];
        // When clients send requests there are no nulls included in the
        // signature input.
        let signature_body = Bytes::from("request data");
        let signature = generate_hmac(&secret[0], "/rpc/service/method", &signature_body);
        // Request bodies are received with a bunch of nulls.
        let request_body = Bytes::from("\0\0\0\0request data");
        let request = Request::builder()
            .uri("http://example.org/rpc/service/method")
            .header("sentry-signature", signature)
            .body(request_body)
            .unwrap();
        let (parts, body) = request.into_parts();

        let res = validate_signature(&secret, &parts, body);
        assert!(res.is_ok());

        // Original body returned.
        let result_body = res.unwrap();
        assert!(result_body == *"\0\0\0\0request data");
    }
}

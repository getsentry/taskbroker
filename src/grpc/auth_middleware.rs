use anyhow::Context;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use hmac::{Hmac, Mac};
use http_body_util::{BodyExt, Full};
use sha2::Sha256;
use std::mem;
use std::task::{self, Poll};
use tower::{Layer, Service};

use crate::config::Config;

// 5 bytes
const HEADER_SIZE: usize =
    // compression flag
    std::mem::size_of::<u8>() +
    // data length
    std::mem::size_of::<u32>();

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
// type TonicBody = UnsyncBoxBody<Bytes, tonic::Status>;
type TonicBody = tonic::body::Body;

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
                    let new_body = Full::new(body);
                    let tb = tonic::body::Body::new(new_body);
                    let new_req = http::Request::from_parts(parts, tb);

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

    // No auth on healthchecks
    if req_head.uri.path().starts_with("/grpc.health.v1.Health") {
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

    // gRPC messages are prefix-length encoded with a 5 byte header
    // https://github.com/hyperium/tonic/blob/master/tonic/src/codec/mod.rs#L93
    // Slice off the header as it won't be included in the signature.
    let trimmed_body = &req_body[HEADER_SIZE..];

    for possible_key in secret {
        let mut hmac = Hmac256::new_from_slice(possible_key.as_bytes()).unwrap();
        hmac.update(req_head.uri.path().as_bytes());
        hmac.update(b":");
        hmac.update(trimmed_body);

        if hmac.verify_slice(&signature[..]).is_ok() {
            return Ok(req_body);
        }
    }

    anyhow::bail!("Invalid request signature")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use hmac::{Hmac, Mac};
    use http::StatusCode;
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
    fn test_validate_signature_health() {
        let secret: Vec<String> = vec!["super secret".into()];
        let request = Request::builder()
            .uri("http://example.org/grpc.health.v1.Health/Watch")
            .header("sentry-signature", "")
            .body(Bytes::from("request data"))
            .unwrap();
        let (parts, body) = request.into_parts();
        let res = validate_signature(&secret, &parts, body);
        assert!(res.is_ok());
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
    fn test_validate_signature_success_header_trim() {
        let secret: Vec<String> = vec!["super secret".into()];
        // Client signatures don't include message framing
        let signature_body = Bytes::from("A request data that is 38 bytes long..");
        let signature = generate_hmac(&secret[0], "/rpc/service/method", &signature_body);

        // Request bodies are length-prefixed with a 5 byte header.
        let request_body = Bytes::from("\0\0\0\0&A request data that is 38 bytes long..");
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
        assert!(result_body == *"\0\0\0\0&A request data that is 38 bytes long..");
    }

    #[test]
    fn test_validate_signature_success_secondary_key() {
        // Check that secondary keys pass validation
        let secret: Vec<String> = vec!["super secret".into(), "second extra secret".into()];
        let signature_body = Bytes::from("A request data that is 38 bytes long..");
        let signature = generate_hmac(&secret[1], "/rpc/service/method", &signature_body);

        // Request bodies are length-prefixed with a 5 byte header.
        let request_body = Bytes::from("\0\0\0\0&A request data that is 38 bytes long..");
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
        assert!(result_body == *"\0\0\0\0&A request data that is 38 bytes long..");
    }

    #[tokio::test]
    async fn test_auth_middleware_no_signature() {
        let mut service = AuthLayer::default().layer(tower::service_fn(|_req| async {
            let body = tonic::body::Body::empty();
            Ok::<_, http::Error>(
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(body)
                    .unwrap(),
            )
        }));

        let req_body = tonic::body::Body::empty();
        let req = http::Request::builder()
            .uri("http://localhost:8080/test")
            .body(req_body)
            .unwrap();

        let res = service.call(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_middleware_signature_success() {
        let secret = vec!["super secret".to_string()];
        let layer = AuthLayer {
            shared_secret: secret,
        };
        let mut service = layer.layer(tower::service_fn(|_req| async {
            let body = tonic::body::Body::empty();
            Ok::<_, http::Error>(
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(body)
                    .unwrap(),
            )
        }));

        // Signature bodies don't include the 5 byte prefix
        let signature_body = Bytes::from("A request data that is 38 bytes long..");
        let signature = generate_hmac(
            &layer.shared_secret[0],
            "/rpc/service/method",
            &signature_body,
        );

        let body_bytes = Bytes::from("\0\0\0\0&A request data that is 38 bytes long..");
        let request_body = tonic::body::Body::new(Full::new(body_bytes));
        let req = http::Request::builder()
            .uri("http://localhost:8080/rpc/service/method")
            .header("sentry-signature", signature)
            .body(request_body)
            .unwrap();

        let res = service.call(req).await.unwrap();

        // No gprc status on success
        assert_eq!(res.status(), StatusCode::OK);
        let headers = res.headers();
        assert!(headers.get("grpc-status").is_none());
    }

    #[tokio::test]
    async fn test_auth_middleware_signature_failure() {
        let secret = vec!["super secret".to_string()];
        let layer = AuthLayer {
            shared_secret: secret,
        };
        let mut service = layer.layer(tower::service_fn(|_req| async {
            let body = tonic::body::Body::empty();
            let res = http::Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .unwrap();
            Ok::<http::Response<_>, http::Error>(res)
        }));

        let body_bytes = Bytes::from("\0\0\0\0&A request data that is 38 bytes long..");
        let request_body = tonic::body::Body::new(Full::new(body_bytes));
        let req = http::Request::builder()
            .uri("http://localhost:8080/rpc/service/method")
            .header("sentry-signature", "lol nope")
            .body(request_body)
            .unwrap();

        let res = service.call(req).await.unwrap();
        let headers = res.headers();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(headers.get("grpc-status").unwrap(), "16");
        assert_eq!(
            headers.get("grpc-message").unwrap(),
            "Authentication%20failed"
        );
    }
}

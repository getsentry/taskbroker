use hmac::Mac;
use http_body_util::combinators::UnsyncBoxBody;
use std::mem;
use std::task::{self, Poll};

use anyhow::Context;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use hmac::Hmac;
use http_body_util::{BodyExt, Full};
use sha2::Sha256;
use tower::{Layer, Service};

use crate::config::Config;

type Hmac256 = Hmac<Sha256>;

#[derive(Debug, Clone, Default)]
pub struct AuthLayer {
    pub shared_secret: Vec<String>,
}

// TODO do I need the layer?
impl AuthLayer {
    pub fn new(config: &Config) -> Self {
        Self {
            shared_secret: config.grpc_shared_secret.clone(),
        }
    }
}

impl <Inner> Layer<Inner> for AuthLayer {
    type Service = AuthService<Inner>;

    fn layer(&self, service: Inner) -> Self::Service {
        AuthService::new(self.shared_secret.clone(), service)
    }
}

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

type TonicBody = UnsyncBoxBody<Bytes, tonic::Status>;

impl<Inner> Service<http::Request<TonicBody>> for AuthService<Inner>
where
    Inner: Service<http::Request<TonicBody>, Response = http::Response<TonicBody>> + Clone + Send + 'static,
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

        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let body_bytes = body.collect().await.unwrap().to_bytes();

            match validate_signature(&secret, &parts, body_bytes).await {
                Ok(body) => {
                    let new_body = Full::new(body);
                    let f = tonic::body::boxed(new_body);
                    let new_req = http::Request::from_parts(parts, f);

                    inner.call(new_req).await
                }
                Err(error) => {
                    tracing::error!("GRPC Authentication error: {error}");
                    let response = tonic::Status::unauthenticated("Authentication failed").into_http();
                    Ok(response)
                }
            }
        })
    }
}


async fn validate_signature(
    secret: &[String],
    req_head: &http::request::Parts,
    req_body: Bytes,
) -> anyhow::Result<Bytes>
{
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

    // TODO: figure out why we get 5 null bytes at the beginning of bodies.
    let req_body_trim = &req_body[5..];

    for possible_key in secret {
        let mut hmac = Hmac256::new_from_slice(possible_key.as_bytes()).unwrap();
        hmac.update(req_head.uri.path().as_bytes());
        hmac.update(b":");
        hmac.update(&req_body_trim);

        if hmac.verify_slice(&signature[..]).is_ok() {
            return Ok(req_body);
        }
    }

    anyhow::bail!("Invalid request signature")
}

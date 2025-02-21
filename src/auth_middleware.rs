use hmac::Mac;
use std::convert::Infallible;
use std::mem;
use std::task::{self, Poll};

use anyhow::Context;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use hmac::Hmac;
use http_body::Body as HttpBody;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use sha2::Sha256;
use tower::{Layer, Service};

use crate::config::Config;

// Gist link https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=44cfd2c852f6e45b41b146a37d80d4b4

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
pub struct AuthService<T> {
    secret: Vec<String>,
    inner: T,
}

impl<T> AuthService<T> {
    pub fn new(secret: Vec<String>, inner: T) -> Self {
        Self { secret, inner }
    }
}

impl<T, B> Service<http::Request<B>> for AuthService<T>
where
    T: Service<http::Request<BoxBody<Bytes, Infallible>>> + Clone + Send + 'static,
    T::Future: Send,
    B: HttpBody<Data: Send, Error: std::error::Error + Send + Sync + 'static> + Send + 'static,
{
    type Response = T::Response;
    type Error = T::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let secret = self.secret.clone();
        let mut inner = self.inner.clone();
        mem::swap(&mut inner, &mut self.inner);

        Box::pin(async move {
            let (parts, body) = req.into_parts();

            match validate_signature(&secret[..], &parts, body).await {
                Ok(body) => {
                    let new_req = http::Request::from_parts(parts, Full::new(body).boxed());
                    inner.call(new_req).await
                }
                Err(error) => {
                    tracing::error!("GRPC Authentication error: {error}");
                    let response = tonic::Status::unauthenticated(error.into()).into_http();
                    Ok(response)
                }
            }
        })
    }
}

async fn validate_signature<B>(
    secret: &[String],
    req_head: &http::request::Parts,
    req_body: B,
) -> anyhow::Result<Bytes>
where
    B: HttpBody<Error: std::error::Error + Send + Sync + 'static>,
{
    let req_body = req_body
        .collect()
        .await
        .context("failed to buffer request body")?
        .to_bytes();

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

    for possible_key in secret {
        let mut hmac = Hmac256::new_from_slice(possible_key.as_bytes()).unwrap();
        hmac.update(req_head.uri.path().as_bytes());
        hmac.update(b":");
        hmac.update(&req_body[..]);

        if hmac.verify_slice(&signature[..]).is_ok() {
            return Ok(req_body);
        }
    }

    anyhow::bail!("Invalid request signature")
}

use anyhow::{anyhow, Error};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use http::HeaderMap;

use pin_project::pin_project;
use tonic::body::{empty_body, BoxBody};
use tower::{Layer, Service};

use crate::config::Config;

#[derive(Debug, Clone, Default)]
pub struct MetricsLayer {}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsMiddleware { inner: service }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsMiddleware<S> {
    inner: S,
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for MetricsMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let start = Instant::now();
            let path = req.uri().path().to_string();

            let response = inner.call(req).await?;

            metrics::counter!("grpc.request", "path" => path.clone()).increment(1);
            metrics::histogram!("grpc.request.duration", "path" => path.clone())
                .record(start.elapsed());

            Ok(response)
        })
    }
}


type HmacSha256 = Hmac<Sha256>;

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

impl <Inner> Layer<Inner> for AuthLayer {
    type Service = AuthService<Inner>;

    fn layer(&self, service: Inner) -> Self::Service {
        AuthService { inner: service, shared_secret: self.shared_secret.clone() }
    }
}

#[derive(Debug, Clone)]
pub struct AuthService<Inner> {
    inner: Inner,
    shared_secret: Vec<String>,
}

impl<Inner> AuthService<Inner> {
    /// Validate the request signature using the path + header map
    /// Currently only the path is used in signing, but the request body should participate as
    /// well to prevent replay attacks as paths are relatively fixed.
    pub fn validate_signature(&self, path: &str, headers: &HeaderMap) -> Result<(), Error> {
        if self.shared_secret.is_empty() {
            return Ok(());
        }
        let header_val = match headers.get("sentry-signature") {
            Some(header_val) => {
                // header value is hex encoded
                let header_str = header_val.to_str().unwrap_or("");
                hex::decode(header_str).unwrap_or(vec![])
            },
            None => {
                return Err(anyhow!("Missing sentry-signature header"));
            }
        };
        // Check all the possible keys in case we are rotating secrets
        for possible_key in &self.shared_secret {
            let mut hmac = HmacSha256::new_from_slice(possible_key.as_bytes()).unwrap();
            hmac.update(path.as_bytes());
            if let Ok(_) = hmac.verify_slice(header_val.as_slice()) {
                return Ok(());
            }
        }
        return Err(anyhow!("Invalid request signature"));
    }
}


impl<Inner, ReqBody> Service<http::Request<ReqBody>> for AuthService<Inner>
where
    Inner: Service<http::Request<ReqBody>, Response = http::Response<BoxBody>>,
{
    type Response = Inner::Response;
    type Error = Inner::Error;
    type Future = AuthResponseFuture<Inner::Future>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let headers = req.headers();
        let path = req.uri().path();
        match self.validate_signature(path, headers) {
            Ok(_) => {
                let future = self.inner.call(req);
                AuthResponseFuture {
                    inner: AuthResponseKind::Success { future },
                }
            },
            Err(err) => {
                AuthResponseFuture {
                    inner: AuthResponseKind::Error { message: err.to_string() },
                }
            }
        }
    }
}

// Define a custom Future because Tower requires a Future to be returned
// and the tonic response futures will not let us return an error response.
#[pin_project]
pub struct AuthResponseFuture<F> {
    #[pin]
    inner: AuthResponseKind<F>,
}

impl<F, E> Future for AuthResponseFuture<F>
where
    F: Future<Output = Result<http::Response<BoxBody>, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.project() {
            AuthResponseKindProj::Success { future } => future.poll(cx),
            AuthResponseKindProj::Error { message: _message } => {
                // let status = tonic::Status::unauthenticated(message.clone());
                let body = empty_body();
                let mut response = http::Response::new(body);
                *response.status_mut() = http::StatusCode::UNAUTHORIZED;

                Poll::Ready(Ok(response))
                // Poll::Ready(Err(status))
            }
        }
    }
}

#[pin_project(project = AuthResponseKindProj)]
enum AuthResponseKind<F> {
    Success {
        #[pin]
        future: F,
    },
    Error {
        message: String,
    }
}

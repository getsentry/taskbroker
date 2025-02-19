use anyhow::{anyhow, Error};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tonic::transport::channel::ResponseFuture;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use http::HeaderMap;
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

impl <S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthService { inner: service, shared_secret: self.shared_secret.clone() }
    }
}

#[derive(Debug, Clone)]
pub struct AuthService<S> {
    inner: S,
    shared_secret: Vec<String>,
}

impl<S> AuthService<S> {
    pub fn validate_signature(&self, path: &str, headers: &HeaderMap) -> Result<(), Error> {
        if self.shared_secret.is_empty() {
            return Ok(());
        }
        let header_val = headers.get("sentry-signature").unwrap_or("").as_bytes();
        let mut success = false;
        for possible_key in &self.shared_secret {
            let mut hmac = HmacSha256::new_from_slice(possible_key.as_bytes()).unwrap();
            hmac.update(path.as_bytes());
            let hmac_bytes = hmac.finalize().into_bytes();
            if hmac_bytes == header_val {
                success = true;
                break;
            }
        }
        if success {
            return Ok(());
        }
        return Err(anyhow!("Invalid request signature"));
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for AuthService<S>
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

        let headers = req.headers();
        let path = req.uri().path();
        match self.validate_signature(path, headers) {
            Ok(_) => {
                Box::pin(async move {
                    let response = inner.call(req).await?;

                    Ok(response)
                })
            },
            Err(err) => {
                let response = http::Response::builder()
                    .status(401)
                    .body(err.to_string())
                    .unwrap();

                Box::pin(async move {
                    Ok(response)
                })
            }
        }
    }
}

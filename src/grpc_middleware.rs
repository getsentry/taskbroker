use anyhow::{anyhow, Error};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use http::HeaderMap;

use pin_project::pin_project;
use tonic::body::BoxBody;
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
    pub fn validate_signature(&self, path: &str, headers: &HeaderMap) -> Result<(), Error> {
        if self.shared_secret.is_empty() {
            return Ok(());
        }
        let header_val = match headers.get("sentry-signature") {
            Some(header_val) => {
                header_val.to_str().unwrap_or("").as_bytes()
            },
            None => {
                return Err(anyhow!("Missing sentry-signature header"));
            }
        };
        let mut success = false;
        for possible_key in &self.shared_secret {
            let mut hmac = HmacSha256::new_from_slice(possible_key.as_bytes()).unwrap();
            hmac.update(path.as_bytes());
            let hmac_bytes: &[u8] = &hmac.finalize().into_bytes();
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

        let auth_res = self.validate_signature(path, headers);
        if let Err(e) = auth_res {
            println!("Authentication failed {:?}", e);
        }

        // TODO remove all the custom future stuff temorarily
        // get things working without auth first.
        // - Then add in the custom future and have wrap
        // the future from inner.call
        // - Then add in the enum and get success working
        // - Then add unauthenticated case to the enum + future
        // - Will also need the custom future to make a response
        let future = self.inner.call(req);
        AuthResponseFuture {
            inner: future,
        }
    }
}

#[pin_project]
pub struct AuthResponseFuture<F> {
    #[pin]
    inner: F,
}

impl<F, E> Future for AuthResponseFuture<F>
where
    F: Future<Output = Result<http::Response<BoxBody>, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/*
#[derive(Debug)]
#[pin_project(project = AuthResponseKindProj)]
enum AuthResponseKind<F> {
    Success {
        #[pin]
        future: F,
    },
    Unauthenticated {
        error: Option<http::Response<BoxBody>>
    }
}

impl<F, E> Future for AuthResponseFuture<F>
where
    F: Future<Output = Result<http::Response<BoxBody>, E>>,
{
    type Output = Result<http::Response<BoxBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.project() {
            AuthResponseKindProj::Success { future } => future.poll(cx),
            AuthResponseKindProj::Unauthenticated { error } => {
                Poll::Ready(Ok(error.take().unwrap()))
            }
        }
    }
}
*/

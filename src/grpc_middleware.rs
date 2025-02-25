use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use tower::{Layer, Service};

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

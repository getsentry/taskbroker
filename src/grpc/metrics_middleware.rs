use std::task::{Context, Poll};
use std::time::Instant;

use futures_util::future::BoxFuture;
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
        let mut inner = self.inner.clone();
        std::mem::swap(&mut inner, &mut self.inner);

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

use anyhow::Result;
use axum::{Router, body::Body, extract::Query, http::Response, response::Html, routing::get};
use pprof::ProfilerGuardBuilder;
use serde::Deserialize;
use std::time::Duration;
use tracing::{error, info};

#[derive(Deserialize)]
struct ProfileParams {
    #[serde(default = "default_seconds")]
    seconds: u64,
}

fn default_seconds() -> u64 {
    30
}

/// Start the pprof HTTP server
pub async fn start_pprof_server(addr: String, port: u16) -> Result<()> {
    let app = Router::new()
        .route("/", get(handle_index))
        .route("/debug/pprof", get(handle_index))
        .route("/debug/pprof/", get(handle_index))
        .route("/debug/pprof/profile", get(pprof_handler))
        .route("/debug/pprof/flamegraph", get(flamegraph_handler));

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", addr, port)).await?;
    info!("pprof server listening on http://{}:{}", addr, port);

    axum::serve(listener, app).await?;
    Ok(())
}

fn generate_flamegraph(seconds: u64) -> Result<Vec<u8>> {
    let guard = ProfilerGuardBuilder::default()
        .frequency(100)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()?;

    std::thread::sleep(Duration::from_secs(seconds));

    let report = guard.report().build()?;
    let mut body = Vec::new();
    report.flamegraph(&mut body)?;

    Ok(body)
}

fn generate_pprof(seconds: u64) -> Result<Vec<u8>> {
    let guard = ProfilerGuardBuilder::default()
        .frequency(100)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()?;

    std::thread::sleep(Duration::from_secs(seconds));

    let report = guard.report().build()?;
    let mut body = Vec::new();
    report.flamegraph(&mut body)?;

    Ok(body)
}

async fn flamegraph_handler(Query(params): Query<ProfileParams>) -> Response<Body> {
    let seconds = params.seconds;
    info!("Starting flamegraph generation for {} seconds", seconds);

    match tokio::task::spawn_blocking(move || generate_flamegraph(seconds)).await {
        Ok(Ok(svg)) => {
            info!("Flamegraph completed, {} bytes", svg.len());
            Response::builder()
                .header("Content-Type", "image/svg+xml")
                .body(Body::from(svg))
                .unwrap()
        }
        Ok(Err(e)) => {
            error!("Failed to generate flamegraph: {:?}", e);
            Response::builder()
                .status(500)
                .body(Body::from(format!("Error: {}", e)))
                .unwrap()
        }
        Err(e) => {
            error!("Task failed: {:?}", e);
            Response::builder()
                .status(500)
                .body(Body::from(format!("Error: {}", e)))
                .unwrap()
        }
    }
}

async fn pprof_handler(Query(params): Query<ProfileParams>) -> Response<Body> {
    let seconds = params.seconds;
    info!("Starting CPU profile for {} seconds", seconds);

    match tokio::task::spawn_blocking(move || generate_pprof(seconds)).await {
        Ok(Ok(data)) => {
            info!("CPU profile completed, {} bytes", data.len());
            Response::builder()
                .header("Content-Type", "application/octet-stream")
                .header("Content-Disposition", "attachment; filename=\"profile.pb\"")
                .body(Body::from(data))
                .unwrap()
        }
        Ok(Err(e)) => {
            error!("Failed to generate profile: {:?}", e);
            Response::builder()
                .status(500)
                .body(Body::from(format!("Error: {}", e)))
                .unwrap()
        }
        Err(e) => {
            error!("Task failed: {:?}", e);
            Response::builder()
                .status(500)
                .body(Body::from(format!("Error: {}", e)))
                .unwrap()
        }
    }
}

async fn handle_index() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>pprof</title>
</head>
<body>
    <h1>Taskbroker pprof</h1>
    <p>Available profiles:</p>
    <ul>
        <li><a href="/debug/pprof/profile?seconds=30">/debug/pprof/profile?seconds=30</a> - CPU profile (default 30s)</li>
        <li><a href="/debug/pprof/flamegraph?seconds=30">/debug/pprof/flamegraph?seconds=30</a> - Flamegraph (default 30s)</li>
    </ul>
</body>
</html>"#,
    )
}

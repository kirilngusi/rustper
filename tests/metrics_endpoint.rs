//! Exercises the Prometheus endpoint over a real socket.

use std::{sync::Arc, time::Duration};

use rustper::metrics::{Metrics, serve};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

/// Asks the OS for a free port, then releases it, so parallel test runs do not
/// collide on a hardcoded one.
async fn free_port() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

async fn get(address: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test]
async fn the_endpoint_serves_scrapeable_metrics() {
    let metrics = Metrics::new(["kafka_in"], ["output_a"]);
    metrics
        .source("kafka_in")
        .events_in
        .store(1234, std::sync::atomic::Ordering::Relaxed);
    metrics
        .sink("output_a")
        .events_dropped
        .store(7, std::sync::atomic::Ordering::Relaxed);

    let address = free_port().await;
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(serve(Arc::clone(&metrics), address, shutdown.child_token()));
    // The listener binds inside the task, so retry briefly rather than sleeping
    // for a fixed guess.
    let mut response = String::new();
    for _ in 0..50 {
        if TcpStream::connect(address).await.is_ok() {
            response = get(address, "/metrics").await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("text/plain"), "{response}");
    assert!(
        response.contains("rustper_source_events_total{source=\"kafka_in\"} 1234"),
        "{response}"
    );
    assert!(
        response.contains("rustper_sink_events_dropped_total{sink=\"output_a\"} 7"),
        "{response}"
    );

    let missing = get(address, "/nope").await;
    assert!(missing.starts_with("HTTP/1.1 404"), "{missing}");

    shutdown.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn binding_a_taken_port_fails_instead_of_silently_doing_nothing() {
    let held = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = held.local_addr().unwrap();
    let error = serve(
        Metrics::new(["s"], ["a"]),
        address,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot bind the metrics listener"),
        "{error:#}"
    );
}

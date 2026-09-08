//! Periodic log reporting, and an optional Prometheus endpoint.

use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, body::Bytes, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::Metrics;

/// Logs one structured line per interval, with rates derived from the delta
/// against the previous snapshot.
///
/// This is the fallback for anyone without a Prometheus scraper: throughput,
/// drops and rebalances stay observable with nothing but `RUST_LOG=info`.
pub async fn report(metrics: Arc<Metrics>, interval: Duration, shutdown: CancellationToken) {
    if interval.is_zero() {
        return;
    }
    let mut previous = metrics.snapshot();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // the first tick fires immediately

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {},
        }
        let current = metrics.snapshot();
        let rates = current.rates_since(&previous);
        for (id, source) in &rates.sources {
            info!(
                component = %id,
                events_per_second = source.events_per_second.round() as u64,
                mib_per_second = source.bytes_per_second / (1024.0 * 1024.0),
                events_total = source.events_total,
                in_flight_batches = source.in_flight_batches,
                in_flight_bytes = source.in_flight_bytes,
                rebalances = source.rebalances,
                "source metrics",
            );
        }
        for (id, sink) in &rates.sinks {
            info!(
                component = %id,
                events_per_second = sink.events_per_second.round() as u64,
                dropped_per_second = sink.dropped_per_second.round() as u64,
                events_total = sink.events_total,
                dropped_total = sink.dropped_total,
                queue_depth = sink.queue_depth,
                retries = sink.retries,
                write_errors = sink.write_errors,
                "sink metrics",
            );
        }
        previous = current;
    }
}

/// Serves `/metrics` until `shutdown` fires.
///
/// Every other path is a 404: this listener exists to be scraped and has no
/// other job.
pub async fn serve(
    metrics: Arc<Metrics>,
    address: SocketAddr,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("cannot bind the metrics listener to {address}"))?;
    info!(%address, "metrics endpoint listening");

    loop {
        let (stream, peer) = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    // A failed accept is not worth taking the router down for.
                    warn!(%error, "metrics listener failed to accept a connection");
                    continue;
                }
            },
        };
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let metrics = Arc::clone(&metrics);
                async move { Ok::<_, Infallible>(route(&metrics, &request)) }
            });
            if let Err(error) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                warn!(%peer, %error, "metrics connection failed");
            }
        });
    }
}

fn route(metrics: &Metrics, request: &Request<hyper::body::Incoming>) -> Response<Full<Bytes>> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
            .body(Full::new(Bytes::from(metrics.render_prometheus())))
            .expect("a valid response"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"try /metrics\n")))
            .expect("a valid response"),
    }
}

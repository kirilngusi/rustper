//! Counters for the router, and the two ways they are reported.
//!
//! Deliberately a plain struct of atomics rather than a metrics framework: the
//! counter set is small and fixed, so a global recorder would buy nothing and
//! cost a dependency plus a layer of indirection on the hot path.
//!
//! Rates are always computed from the delta between two [`Snapshot`]s. A
//! cumulative average since process start is dragged down by ramp-up and hides
//! stalls entirely, which is exactly how a 25-second consumption stall went
//! unnoticed during benchmarking.

mod server;

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub use server::{report, serve};

/// Relaxed ordering throughout: these counters are statistics, never
/// synchronisation. Nothing reads them to decide control flow.
const ORDER: Ordering = Ordering::Relaxed;

#[derive(Debug, Default)]
pub struct SourceMetrics {
    pub events_in: AtomicU64,
    pub bytes_in: AtomicU64,
    pub batches_in: AtomicU64,
    pub commits: AtomicU64,
    /// Every rebalance stalls consumption. Without this counter the stall is
    /// invisible from inside the process and only shows up in the broker log.
    ///
    /// Counts the initial assignment too, so this is never zero on a healthy
    /// consumer; what matters is the rate, not the total.
    pub rebalances: AtomicU64,
    pub in_flight_bytes: AtomicU64,
    pub in_flight_batches: AtomicU64,
}

#[derive(Debug, Default)]
pub struct SinkMetrics {
    pub events_out: AtomicU64,
    /// Rows the sink refused to write, for example a payload that is not a JSON
    /// object. These are lost by design; this counter is what makes the loss
    /// visible rather than silent.
    pub events_dropped: AtomicU64,
    pub batches_written: AtomicU64,
    pub write_errors: AtomicU64,
    pub retries: AtomicU64,
    pub queue_depth: AtomicU64,
}

#[derive(Debug)]
pub struct Metrics {
    sources: BTreeMap<String, SourceMetrics>,
    sinks: BTreeMap<String, SinkMetrics>,
}

impl Metrics {
    pub fn new<'a>(
        sources: impl IntoIterator<Item = &'a str>,
        sinks: impl IntoIterator<Item = &'a str>,
    ) -> Arc<Self> {
        Arc::new(Self {
            sources: sources
                .into_iter()
                .map(|id| (id.to_owned(), SourceMetrics::default()))
                .collect(),
            sinks: sinks
                .into_iter()
                .map(|id| (id.to_owned(), SinkMetrics::default()))
                .collect(),
        })
    }

    /// Returns the counters for `id`.
    ///
    /// Panics on an unknown id: every component is registered at startup from
    /// the same config, so a miss is a wiring bug, not a runtime condition.
    pub fn source(&self, id: &str) -> &SourceMetrics {
        self.sources
            .get(id)
            .unwrap_or_else(|| panic!("source {id:?} was not registered"))
    }

    pub fn sink(&self, id: &str) -> &SinkMetrics {
        self.sinks
            .get(id)
            .unwrap_or_else(|| panic!("sink {id:?} was not registered"))
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            taken_at: Instant::now(),
            sources: self
                .sources
                .iter()
                .map(|(id, m)| {
                    (
                        id.clone(),
                        SourceValues {
                            events_in: m.events_in.load(ORDER),
                            bytes_in: m.bytes_in.load(ORDER),
                            batches_in: m.batches_in.load(ORDER),
                            commits: m.commits.load(ORDER),
                            rebalances: m.rebalances.load(ORDER),
                            in_flight_bytes: m.in_flight_bytes.load(ORDER),
                            in_flight_batches: m.in_flight_batches.load(ORDER),
                        },
                    )
                })
                .collect(),
            sinks: self
                .sinks
                .iter()
                .map(|(id, m)| {
                    (
                        id.clone(),
                        SinkValues {
                            events_out: m.events_out.load(ORDER),
                            events_dropped: m.events_dropped.load(ORDER),
                            batches_written: m.batches_written.load(ORDER),
                            write_errors: m.write_errors.load(ORDER),
                            retries: m.retries.load(ORDER),
                            queue_depth: m.queue_depth.load(ORDER),
                        },
                    )
                })
                .collect(),
        }
    }

    /// Renders the Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let mut out = String::with_capacity(1024);
        for (metric, help, kind, extract) in SOURCE_SERIES {
            writeln!(out, "# HELP {metric} {help}").expect("writing to a String");
            writeln!(out, "# TYPE {metric} {kind}").expect("writing to a String");
            for (id, values) in &snapshot.sources {
                writeln!(out, "{metric}{{source=\"{id}\"}} {}", extract(values))
                    .expect("writing to a String");
            }
        }
        for (metric, help, kind, extract) in SINK_SERIES {
            writeln!(out, "# HELP {metric} {help}").expect("writing to a String");
            writeln!(out, "# TYPE {metric} {kind}").expect("writing to a String");
            for (id, values) in &snapshot.sinks {
                writeln!(out, "{metric}{{sink=\"{id}\"}} {}", extract(values))
                    .expect("writing to a String");
            }
        }
        out
    }
}

type SourceSeries = (
    &'static str,
    &'static str,
    &'static str,
    fn(&SourceValues) -> u64,
);
type SinkSeries = (
    &'static str,
    &'static str,
    &'static str,
    fn(&SinkValues) -> u64,
);

const SOURCE_SERIES: &[SourceSeries] = &[
    (
        "rustper_source_events_total",
        "Events consumed and committed.",
        "counter",
        |v| v.events_in,
    ),
    (
        "rustper_source_bytes_total",
        "Payload bytes consumed.",
        "counter",
        |v| v.bytes_in,
    ),
    (
        "rustper_source_batches_total",
        "Batches dispatched to the fan-out.",
        "counter",
        |v| v.batches_in,
    ),
    (
        "rustper_source_commits_total",
        "Offset commits issued.",
        "counter",
        |v| v.commits,
    ),
    (
        "rustper_source_rebalances_total",
        "Consumer group rebalances observed.",
        "counter",
        |v| v.rebalances,
    ),
    (
        "rustper_source_in_flight_bytes",
        "Bytes held by unacknowledged batches.",
        "gauge",
        |v| v.in_flight_bytes,
    ),
    (
        "rustper_source_in_flight_batches",
        "Unacknowledged batches.",
        "gauge",
        |v| v.in_flight_batches,
    ),
];

const SINK_SERIES: &[SinkSeries] = &[
    (
        "rustper_sink_events_total",
        "Events written.",
        "counter",
        |v| v.events_out,
    ),
    (
        "rustper_sink_events_dropped_total",
        "Events the sink refused to write.",
        "counter",
        |v| v.events_dropped,
    ),
    (
        "rustper_sink_batches_total",
        "Coalesced writes completed.",
        "counter",
        |v| v.batches_written,
    ),
    (
        "rustper_sink_write_errors_total",
        "Writes that failed after retries.",
        "counter",
        |v| v.write_errors,
    ),
    (
        "rustper_sink_retries_total",
        "Write attempts retried.",
        "counter",
        |v| v.retries,
    ),
    (
        "rustper_sink_queue_depth",
        "Envelopes waiting in the sink queue.",
        "gauge",
        |v| v.queue_depth,
    ),
];

#[derive(Debug, Clone, Copy, Default)]
pub struct SourceValues {
    pub events_in: u64,
    pub bytes_in: u64,
    pub batches_in: u64,
    pub commits: u64,
    pub rebalances: u64,
    pub in_flight_bytes: u64,
    pub in_flight_batches: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SinkValues {
    pub events_out: u64,
    pub events_dropped: u64,
    pub batches_written: u64,
    pub write_errors: u64,
    pub retries: u64,
    pub queue_depth: u64,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub taken_at: Instant,
    pub sources: BTreeMap<String, SourceValues>,
    pub sinks: BTreeMap<String, SinkValues>,
}

/// Per-second rates between two snapshots.
#[derive(Debug, Clone)]
pub struct Rates {
    pub elapsed: Duration,
    pub sources: BTreeMap<String, SourceRates>,
    pub sinks: BTreeMap<String, SinkRates>,
}

#[derive(Debug, Clone, Copy)]
pub struct SourceRates {
    pub events_per_second: f64,
    pub bytes_per_second: f64,
    pub rebalances: u64,
    pub in_flight_bytes: u64,
    pub in_flight_batches: u64,
    pub events_total: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct SinkRates {
    pub events_per_second: f64,
    pub dropped_per_second: f64,
    pub retries: u64,
    pub write_errors: u64,
    pub queue_depth: u64,
    pub events_total: u64,
    pub dropped_total: u64,
}

impl Snapshot {
    /// Rates of `self` relative to the earlier snapshot `previous`.
    ///
    /// Counters are monotonic, so a difference can only underflow if the two
    /// snapshots are passed the wrong way round; `saturating_sub` turns that
    /// into a zero rather than a panic in a reporting path.
    pub fn rates_since(&self, previous: &Self) -> Rates {
        let elapsed = self.taken_at.saturating_duration_since(previous.taken_at);
        // A zero-length interval would divide by zero. Report zero instead:
        // no time has passed, so no rate is meaningful.
        let seconds = elapsed.as_secs_f64();
        let per_second = |current: u64, before: u64| {
            if seconds > 0.0 {
                current.saturating_sub(before) as f64 / seconds
            } else {
                0.0
            }
        };
        Rates {
            elapsed,
            sources: self
                .sources
                .iter()
                .map(|(id, now)| {
                    let was = previous.sources.get(id).copied().unwrap_or_default();
                    (
                        id.clone(),
                        SourceRates {
                            events_per_second: per_second(now.events_in, was.events_in),
                            bytes_per_second: per_second(now.bytes_in, was.bytes_in),
                            rebalances: now.rebalances,
                            in_flight_bytes: now.in_flight_bytes,
                            in_flight_batches: now.in_flight_batches,
                            events_total: now.events_in,
                        },
                    )
                })
                .collect(),
            sinks: self
                .sinks
                .iter()
                .map(|(id, now)| {
                    let was = previous.sinks.get(id).copied().unwrap_or_default();
                    (
                        id.clone(),
                        SinkRates {
                            events_per_second: per_second(now.events_out, was.events_out),
                            dropped_per_second: per_second(now.events_dropped, was.events_dropped),
                            retries: now.retries,
                            write_errors: now.write_errors,
                            queue_depth: now.queue_depth,
                            events_total: now.events_out,
                            dropped_total: now.events_dropped,
                        },
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> Arc<Metrics> {
        Metrics::new(["src"], ["a", "b"])
    }

    #[test]
    fn rates_come_from_the_delta_not_the_running_total() {
        let metrics = metrics();
        // A slow first window followed by a fast one. A cumulative average
        // would blend them; the interval rate must report only the second.
        metrics.source("src").events_in.store(1_000, ORDER);
        let first = metrics.snapshot();
        metrics.source("src").events_in.store(11_000, ORDER);
        let mut second = metrics.snapshot();
        second.taken_at = first.taken_at + Duration::from_secs(2);

        let rates = second.rates_since(&first);
        assert_eq!(rates.sources["src"].events_per_second, 5_000.0);
        assert_eq!(rates.sources["src"].events_total, 11_000);
    }

    #[test]
    fn a_zero_length_interval_reports_zero_instead_of_dividing_by_zero() {
        let metrics = metrics();
        let first = metrics.snapshot();
        metrics.source("src").events_in.store(500, ORDER);
        let mut second = metrics.snapshot();
        second.taken_at = first.taken_at;

        let rates = second.rates_since(&first);
        assert_eq!(rates.sources["src"].events_per_second, 0.0);
        assert!(rates.elapsed.is_zero());
    }

    #[test]
    fn snapshots_passed_in_the_wrong_order_do_not_panic() {
        let metrics = metrics();
        metrics.sink("a").events_out.store(10, ORDER);
        let later = metrics.snapshot();
        metrics.sink("a").events_out.store(0, ORDER);
        let mut earlier = metrics.snapshot();
        earlier.taken_at = later.taken_at + Duration::from_secs(1);

        // saturating_sub keeps a reporting path from bringing the router down.
        assert_eq!(
            earlier.rates_since(&later).sinks["a"].events_per_second,
            0.0
        );
    }

    #[test]
    fn dropped_rows_are_reported_separately_from_written_ones() {
        let metrics = metrics();
        let first = metrics.snapshot();
        metrics.sink("a").events_out.store(90, ORDER);
        metrics.sink("a").events_dropped.store(10, ORDER);
        let mut second = metrics.snapshot();
        second.taken_at = first.taken_at + Duration::from_secs(1);

        let rates = second.rates_since(&first);
        assert_eq!(rates.sinks["a"].events_per_second, 90.0);
        assert_eq!(rates.sinks["a"].dropped_per_second, 10.0);
        assert_eq!(rates.sinks["a"].dropped_total, 10);
    }

    #[test]
    fn prometheus_output_labels_every_component() {
        let metrics = metrics();
        metrics.source("src").events_in.store(7, ORDER);
        metrics.sink("b").events_dropped.store(3, ORDER);
        let text = metrics.render_prometheus();

        assert!(
            text.contains("rustper_source_events_total{source=\"src\"} 7"),
            "{text}"
        );
        assert!(
            text.contains("rustper_sink_events_dropped_total{sink=\"b\"} 3"),
            "{text}"
        );
        assert!(
            text.contains("rustper_sink_events_dropped_total{sink=\"a\"} 0"),
            "{text}"
        );
        // Every series needs its HELP and TYPE lines to be valid exposition.
        for series in SOURCE_SERIES {
            assert!(
                text.contains(&format!("# TYPE {} ", series.0)),
                "missing TYPE for {}",
                series.0
            );
        }
        for series in SINK_SERIES {
            assert!(
                text.contains(&format!("# HELP {} ", series.0)),
                "missing HELP for {}",
                series.0
            );
        }
    }

    #[test]
    fn every_exposed_series_name_is_unique() {
        let mut names: Vec<&str> = SOURCE_SERIES
            .iter()
            .map(|s| s.0)
            .chain(SINK_SERIES.iter().map(|s| s.0))
            .collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            total,
            "duplicate metric name would break scraping"
        );
    }

    #[test]
    #[should_panic(expected = "was not registered")]
    fn an_unregistered_component_is_a_wiring_bug() {
        metrics().sink("nope");
    }
}

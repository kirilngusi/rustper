use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, anyhow, bail};
use futures::future::try_join_all;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{config::Config, event::EventBatch, metrics::Metrics, sink, source};

pub struct SinkEnvelope {
    batch: Arc<EventBatch>,
    ack: BranchAck,
}

impl SinkEnvelope {
    pub fn batch(&self) -> &Arc<EventBatch> {
        &self.batch
    }
    pub fn delivered(self) {
        self.ack.complete(true);
    }
    pub fn failed(self) {
        self.ack.complete(false);
    }
}

struct AckState {
    remaining: usize,
    delivered: bool,
    notify: Option<oneshot::Sender<bool>>,
}

struct BranchAck {
    state: Arc<Mutex<AckState>>,
    completed: AtomicBool,
}

impl BranchAck {
    fn complete(&self, delivered: bool) {
        if self.completed.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut state = self.state.lock().expect("ack mutex poisoned");
        state.delivered &= delivered;
        state.remaining -= 1;
        if state.remaining == 0
            && let Some(notify) = state.notify.take()
        {
            let _ = notify.send(state.delivered);
        }
    }
}

impl Drop for BranchAck {
    fn drop(&mut self) {
        self.complete(false);
    }
}

#[derive(Clone)]
pub struct Fanout {
    destinations: Arc<Vec<mpsc::Sender<SinkEnvelope>>>,
}

impl Fanout {
    pub fn new(destinations: Vec<mpsc::Sender<SinkEnvelope>>) -> Self {
        Self {
            destinations: Arc::new(destinations),
        }
    }

    pub async fn send(&self, batch: EventBatch) -> Result<()> {
        if self.destinations.is_empty() {
            return Ok(());
        }
        let (notify, delivered) = oneshot::channel();
        let state = Arc::new(Mutex::new(AckState {
            remaining: self.destinations.len(),
            delivered: true,
            notify: Some(notify),
        }));
        let batch = Arc::new(batch);
        let sends = self.destinations.iter().map(|destination| {
            let envelope = SinkEnvelope {
                batch: Arc::clone(&batch),
                ack: BranchAck {
                    state: Arc::clone(&state),
                    completed: AtomicBool::new(false),
                },
            };
            async move {
                destination
                    .send(envelope)
                    .await
                    .map_err(|_| anyhow!("sink task stopped"))
            }
        });
        try_join_all(sends).await?;
        match delivered.await {
            Ok(true) => Ok(()),
            Ok(false) => bail!("one or more sinks rejected the batch"),
            Err(_) => bail!("acknowledgement channel closed"),
        }
    }
}

/// Waits for every task in `tasks`, returning the first failure.
async fn join_all(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    while let Some(joined) = tasks.join_next().await {
        joined??;
    }
    Ok(())
}

/// Resolves on the first task failure, and never resolves otherwise.
///
/// A sink that exits cleanly is not itself a reason to tear the topology down;
/// the sources notice through their closed channels.
async fn first_failure(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    loop {
        match tasks.join_next().await {
            Some(joined) => joined??,
            None => std::future::pending::<()>().await,
        }
    }
}

pub async fn run(config: Config) -> Result<()> {
    config.validate()?;
    let shutdown = CancellationToken::new();
    let metrics = Metrics::new(
        config.sources.keys().map(String::as_str),
        config.sinks.keys().map(String::as_str),
    );
    let mut sink_tasks = JoinSet::new();
    let mut sink_inputs = HashMap::new();

    for (id, sink_config) in &config.sinks {
        let capacity = sink_config.buffer_capacity();
        let (tx, rx) = mpsc::channel(capacity);
        sink_inputs.insert(id.clone(), tx);
        let component = sink::build(id, sink_config)?;
        let settings = sink_config.runtime_settings();
        let token = shutdown.child_token();
        let sink_metrics = Arc::clone(&metrics);
        sink_tasks
            .spawn(async move { sink::run(component, settings, sink_metrics, rx, token).await });
    }

    let mut source_tasks = JoinSet::new();
    for (id, source_config) in &config.sources {
        let destinations = config
            .sinks
            .iter()
            .filter(|(_, sink)| sink.inputs().iter().any(|input| input == id))
            .map(|(sink_id, _)| sink_inputs[sink_id].clone())
            .collect();
        let component = source::build(id, source_config, Arc::clone(&metrics))?;
        let fanout = Fanout::new(destinations);
        let token = shutdown.child_token();
        source_tasks.spawn(async move { component.run(fanout, token).await });
    }
    drop(sink_inputs);

    // Reporting is spawned outside the source/sink JoinSets: a failure to bind
    // the metrics port should not be mistaken for a pipeline failure, and the
    // reporter must keep running while sinks drain.
    let mut observability = JoinSet::new();
    observability.spawn(crate::metrics::report(
        Arc::clone(&metrics),
        config.metrics.log_interval(),
        shutdown.child_token(),
    ));
    if let Some(address) = config.metrics.listen {
        let metrics = Arc::clone(&metrics);
        let token = shutdown.child_token();
        observability.spawn(async move {
            if let Err(error) = crate::metrics::serve(metrics, address, token).await {
                tracing::error!(%error, "metrics endpoint stopped");
            }
        });
    }

    let outcome = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
            Ok(())
        },
        result = join_all(&mut source_tasks) => result,
        result = first_failure(&mut sink_tasks) => result,
    };

    shutdown.cancel();
    source_tasks.shutdown().await;
    // Sinks are given the chance to finish their in-flight writes; their
    // acknowledgements are what keep the at-least-once contract honest.
    let drained = join_all(&mut sink_tasks).await;
    observability.shutdown().await;
    outcome.and(drained)
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::Fanout;
    use crate::event::{Event, EventBatch, SourceMetadata};

    fn batch() -> EventBatch {
        EventBatch::new(vec![Event {
            key: None,
            payload: "hello".into(),
            timestamp_ms: None,
            source: SourceMetadata {
                component_id: "test".into(),
                topic: "input".into(),
                partition: 0,
                offset: 0,
            },
        }])
    }

    #[tokio::test]
    async fn waits_for_every_fanout_branch() {
        let (first_tx, mut first_rx) = mpsc::channel(1);
        let (second_tx, mut second_rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![first_tx, second_tx]);
        let send = tokio::spawn(async move { fanout.send(batch()).await });

        first_rx.recv().await.unwrap().delivered();
        assert!(
            !send.is_finished(),
            "second branch has not acknowledged yet"
        );
        second_rx.recv().await.unwrap().delivered();
        assert!(send.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn one_failed_branch_rejects_the_batch() {
        let (first_tx, mut first_rx) = mpsc::channel(1);
        let (second_tx, mut second_rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![first_tx, second_tx]);
        let send = tokio::spawn(async move { fanout.send(batch()).await });

        first_rx.recv().await.unwrap().delivered();
        second_rx.recv().await.unwrap().failed();
        assert!(send.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn a_dropped_envelope_fails_the_batch() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let send = tokio::spawn(async move { fanout.send(batch()).await });
        drop(rx.recv().await.unwrap());
        assert!(send.await.unwrap().is_err());
    }
}

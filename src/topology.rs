use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow, bail};
use futures::future::try_join_all;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{config::Config, event::Event, sink, source};

pub type EventBatch = Arc<Vec<Event>>;

pub struct SinkEnvelope {
    pub events: EventBatch,
    ack: BranchAck,
}

impl SinkEnvelope {
    pub fn delivered(mut self) {
        self.ack.complete(true);
    }
    pub fn failed(mut self) {
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
    completed: bool,
}

impl BranchAck {
    fn complete(&mut self, delivered: bool) {
        if self.completed {
            return;
        }
        self.completed = true;
        let mut state = self.state.lock().expect("ack mutex poisoned");
        state.delivered &= delivered;
        state.remaining -= 1;
        if state.remaining == 0 {
            if let Some(notify) = state.notify.take() {
                let _ = notify.send(state.delivered);
            }
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
    pub async fn send(&self, events: Vec<Event>) -> Result<()> {
        if self.destinations.is_empty() {
            return Ok(());
        }
        let (notify, delivered) = oneshot::channel();
        let state = Arc::new(Mutex::new(AckState {
            remaining: self.destinations.len(),
            delivered: true,
            notify: Some(notify),
        }));
        let events = Arc::new(events);
        let sends = self.destinations.iter().map(|destination| {
            let envelope = SinkEnvelope {
                events: Arc::clone(&events),
                ack: BranchAck {
                    state: Arc::clone(&state),
                    completed: false,
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

pub async fn run(config: Config) -> Result<()> {
    config.validate()?;
    let shutdown = CancellationToken::new();
    let mut sink_tasks = JoinSet::new();
    let mut sink_inputs = HashMap::new();

    for (id, sink_config) in &config.sinks {
        let capacity = sink_config.buffer_capacity();
        let (tx, rx) = mpsc::channel(capacity);
        sink_inputs.insert(id.clone(), tx);
        let component = sink::build(id, sink_config)?;
        let token = shutdown.child_token();
        sink_tasks.spawn(async move { sink::run(component, rx, token).await });
    }

    let mut source_tasks = JoinSet::new();
    for (id, source_config) in &config.sources {
        let destinations = config
            .sinks
            .iter()
            .filter(|(_, sink)| sink.inputs().iter().any(|input| input == id))
            .map(|(sink_id, _)| sink_inputs[sink_id].clone())
            .collect();
        let component = source::build(id, source_config)?;
        let fanout = Fanout {
            destinations: Arc::new(destinations),
        };
        let token = shutdown.child_token();
        source_tasks.spawn(async move { component.run(fanout, token).await });
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        result = source_tasks.join_next() => {
            match result {
                Some(Ok(Ok(()))) | None => {},
                Some(Ok(Err(error))) => return Err(error),
                Some(Err(error)) => return Err(error.into()),
            }
        }
        result = sink_tasks.join_next() => {
            match result {
                Some(Ok(Ok(()))) | None => {},
                Some(Ok(Err(error))) => return Err(error),
                Some(Err(error)) => return Err(error.into()),
            }
        }
    }
    shutdown.cancel();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::Fanout;
    use crate::event::{Event, SourceMetadata};

    fn event() -> Event {
        Event {
            key: None,
            payload: "hello".into(),
            timestamp_ms: None,
            source: SourceMetadata {
                component_id: "test".into(),
                topic: "input".into(),
                partition: 0,
                offset: 0,
            },
        }
    }

    #[tokio::test]
    async fn waits_for_every_fanout_branch() {
        let (first_tx, mut first_rx) = mpsc::channel(1);
        let (second_tx, mut second_rx) = mpsc::channel(1);
        let fanout = Fanout {
            destinations: Arc::new(vec![first_tx, second_tx]),
        };
        let send = tokio::spawn(async move { fanout.send(vec![event()]).await });

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
        let fanout = Fanout {
            destinations: Arc::new(vec![first_tx, second_tx]),
        };
        let send = tokio::spawn(async move { fanout.send(vec![event()]).await });

        first_rx.recv().await.unwrap().delivered();
        second_rx.recv().await.unwrap().failed();
        assert!(send.await.unwrap().is_err());
    }
}

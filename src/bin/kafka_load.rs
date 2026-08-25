use std::{env, time::Instant};

use anyhow::{Context, Result};
use futures::{StreamExt, stream};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};

#[tokio::main]
async fn main() -> Result<()> {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| "rustper-input".into());
    let count: u64 = env::var("MESSAGE_COUNT")
        .unwrap_or_else(|_| "100000".into())
        .parse()
        .context("invalid MESSAGE_COUNT")?;
    let concurrency: usize = env::var("CONCURRENCY")
        .unwrap_or_else(|_| "1000".into())
        .parse()
        .context("invalid CONCURRENCY")?;
    let payload_size: usize = env::var("PAYLOAD_SIZE")
        .unwrap_or_else(|_| "1024".into())
        .parse()
        .context("invalid PAYLOAD_SIZE")?;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("enable.idempotence", "true")
        .set("compression.type", "lz4")
        .set("linger.ms", "5")
        .create()
        .context("cannot create load producer")?;
    let payload = vec![42_u8; payload_size];
    let started = Instant::now();

    stream::iter(0..count)
        .map(|id| {
            let producer = producer.clone();
            let topic = topic.clone();
            let payload = &payload;
            async move {
                let key = id.to_be_bytes();
                producer
                    .send(
                        FutureRecord::to(&topic).key(&key).payload(payload),
                        Timeout::Never,
                    )
                    .await
                    .map_err(|(error, _)| error)
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let elapsed = started.elapsed().as_secs_f64();
    println!("produced:   {count}");
    println!("elapsed:    {elapsed:.3} s");
    println!("throughput: {:.0} msg/s", count as f64 / elapsed);
    Ok(())
}

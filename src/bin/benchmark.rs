use std::time::Instant;

use bytes::Bytes;
use rustper::{Message, fan_out, spawn_sink};

const MESSAGE_COUNT: u64 = 1_000_000;
const PAYLOAD_SIZE: usize = 1024;
const OUTPUT_COUNT: usize = 3;
const CHANNEL_CAPACITY: usize = 16_384;

#[tokio::main]
async fn main() {
    let mut outputs = Vec::with_capacity(OUTPUT_COUNT);
    let mut workers = Vec::with_capacity(OUTPUT_COUNT);

    for _ in 0..OUTPUT_COUNT {
        let (sender, worker) = spawn_sink(CHANNEL_CAPACITY);
        outputs.push(sender);
        workers.push(worker);
    }

    // Bytes::clone cũng chia sẻ vùng nhớ, không copy 1 KiB cho mỗi message.
    let payload = Bytes::from(vec![42_u8; PAYLOAD_SIZE]);
    let messages =
        (0..MESSAGE_COUNT).map(|id| Message::new(id.to_be_bytes().to_vec(), payload.clone()));

    let started = Instant::now();
    let input_count = fan_out(messages, &outputs)
        .await
        .expect("output worker stopped unexpectedly");
    drop(outputs);

    let mut deliveries = 0_u64;
    let mut delivered_bytes = 0_u64;
    for worker in workers {
        let stats = worker.await.expect("output worker panicked");
        deliveries += stats.messages;
        delivered_bytes += stats.bytes;
    }

    let elapsed = started.elapsed();
    let seconds = elapsed.as_secs_f64();

    println!("messages in:       {input_count}");
    println!("outputs:           {OUTPUT_COUNT}");
    println!("total deliveries:  {deliveries}");
    println!("elapsed:           {seconds:.3} s");
    println!(
        "input throughput:  {:.0} msg/s",
        input_count as f64 / seconds
    );
    println!(
        "output throughput: {:.0} deliveries/s",
        deliveries as f64 / seconds
    );
    println!(
        "logical payload:    {:.2} GiB/s",
        delivered_bytes as f64 / seconds / 1024_f64.powi(3)
    );
}

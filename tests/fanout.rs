use rustper::{Message, SinkStats, fan_out, spawn_sink};

#[tokio::test]
async fn every_sink_receives_every_message() {
    let (first_tx, first_worker) = spawn_sink(2);
    let (second_tx, second_worker) = spawn_sink(2);
    let outputs = vec![first_tx, second_tx];

    let messages = (0..10).map(|id| Message::new(format!("key-{id}"), "hello"));
    let input_count = fan_out(messages, &outputs).await.unwrap();
    drop(outputs);

    let expected = SinkStats {
        messages: 10,
        bytes: 50,
    };

    assert_eq!(input_count, 10);
    assert_eq!(first_worker.await.unwrap(), expected);
    assert_eq!(second_worker.await.unwrap(), expected);
}

#[tokio::test]
async fn no_outputs_is_valid() {
    let messages = [Message::new("key", "payload")];
    let input_count = fan_out(messages, &[]).await.unwrap();

    assert_eq!(input_count, 1);
}

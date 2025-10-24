# MQTTier

A Rust MQTT client library providing an abstracted interface around [rumqttc](https://github.com/bytebeamio/rumqtt).

## Features

- **Asynchronous**: Built on top of tokio for async/await support
- **Serialization**: Uses serde for JSON serialization of published messages
- **Logging**: Integrated with the standard Rust logging ecosystem
- **Reconnection**: Automatic reconnection on connection loss
- **Queueing**: Messages and subscriptions are queued when disconnected (or before connection is made.)
- **Cloneable**: Client can be cloned and shared across tasks
- **MQTT 5.0**: Uses MQTT version 5 protocol

## Usage

Add this to your `Cargo.toml` (use the current crate version):

```toml
[dependencies]
mqttier = "0.2"
```


### Basic Example

This example has been abbreviated to focus on library-specific calls.

```rust

// Create a new MQTT client
let client = MqttierClient::new("localhost", 1883, Some("mqttier_example".to_string())).unwrap();

// Start the run loop (spawned background tasks). Call once per client.
client.start().await.unwrap();

// Create mpsc channel for receiving messages
let (message_tx, mut message_rx) = mpsc::channel::<ReceivedMessage>(64);

// Subscribe to a topic. We pass the tx-side of the channel that will receive messages
// for this subscription. The function returns a subscription id (`usize`).
let subscription_id = client.subscribe("test/topic".to_string(), 0, message_tx).await.unwrap();

// Start a task to handle incoming messages
tokio::spawn(async move {
    while let Some(message) = message_rx.recv().await {
        if let Ok(test_msg) = serde_json::from_slice::<TestMessage>(&message.payload) {
            println!("Parsed message: {:?}", test_msg);
        }
    }
});

// Publish a serializable structure and wait for its publish completion
let completion_rx = client.publish_structure("test/topic".to_string(), &test_message).await.unwrap();
match completion_rx.await {
    Ok(result) => println!("Publish result: {:?}", result),
    Err(_) => println!("Publish completion channel closed"),
}

```

## API

### MqttierClient

#### `new(hostname: &str, port: u16, client_id: Option<String>) -> Result<Self>`

Creates a new MQTT client.

- `hostname`: The MQTT broker hostname
- `port`: The MQTT broker port
- `client_id`: Optional client ID. If `None`, a random UUID is generated

#### `start() -> Result<()>`

Starts the background run loop for connections and publishing. This should be called once per client. If already running, this method does nothing.

#### `subscribe(topic: String, qos: u8, message_tx: mpsc::Sender<ReceivedMessage>) -> Result<usize>`

Subscribes to a topic and returns a subscription ID (`usize`).

- `topic`: The MQTT topic to subscribe to
- `qos`: The Quality of Service level (0, 1, or 2)
- `message_tx`: The sender channel for delivering received messages (bounded channel with capacity you choose)
- Returns: The subscription ID (`usize`)



#### `publish_structure<T: Serialize>(topic: String, payload: T) -> Result<oneshot::Receiver<PublishResult>>`

Publishes a serializable struct as JSON to a topic and returns a `oneshot::Receiver<PublishResult>` which will resolve with the publish outcome.

- `topic`: The MQTT topic to publish to
- `payload`: The struct to publish (must implement `Serialize`)

#### `publish_request<T: Serialize>(topic: String, payload: T, response_topic: String, correlation_id: Vec<u8>) -> Result<oneshot::Receiver<PublishResult>>`

Publishes a request message with response topic and correlation ID.

- `topic`: The MQTT topic to publish to
- `payload`: The struct to publish
- `response_topic`: The topic for responses
- `correlation_id`: Correlation ID for matching responses (`Vec<u8>`)

#### `publish_response<T: Serialize>(topic: String, payload: T, correlation_id: Vec<u8>) -> Result<oneshot::Receiver<PublishResult>>`

Publishes a response message with correlation ID.

- `topic`: The MQTT topic to publish to
- `payload`: The struct to publish
- `correlation_id`: Correlation ID for matching requests

#### `publish_state<T: Serialize>(topic: String, payload: T, state_version: u32) -> Result<oneshot::Receiver<PublishResult>>`

Publishes a state message with a version property.

- `topic`: The MQTT topic to publish to
- `payload`: The struct to publish
- `state_version`: Version number for the state

## Behavior

### Automatic Reconnection

The client automatically reconnects when the connection is lost, with a 5-second delay between attempts.

### Message Queueing

When disconnected:

- Subscription requests are queued and processed when reconnected
- Publish requests are queued and sent when reconnected

### Logging

The library emits tracing events using the `tracing` crate. Initialize a tracing subscriber in your application, for example:

```rust
tracing_subscriber::fmt().with_env_filter("info").init();
```

## Example

See `examples/basic_usage.rs` for a complete example.

Run the example with:

```bash
cargo run --example basic_usage
```

## License

This project is licensed under the LGPLv2 License. (Not v3!)

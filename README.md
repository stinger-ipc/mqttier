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

Add this to your `Cargo.toml`:

```toml
[dependencies]
mqttier = "0.1.0"
```


### Basic Example

This example has been abbreviated to focus on library-specific calls.

```rust

// Create a new MQTT client
let client = MqttierClient::new("localhost", 1883, Some("mqttier_example".to_string()));

// Start the run loop
client.run_loop().await;

// Create mpsc channel for receiving messages
let (message_tx, mut message_rx) = mpsc::unbounded_channel::<ReceivedMessage>();

// Subscribe to a topic.  We pass in the tx-side of the channel that we want to be used
// when a message is received for a subscription.  
let subscription_id = client.subscribe("test/topic".to_string(), QoS::AtMostOnce, message_tx).await;

// Start a task to handle incoming messages
tokio::spawn(async move {
    while let Some(message) = message_rx.recv().await {
        if let Ok(test_msg) = serde_json::from_slice::<TestMessage>(&message.payload) {
            println!("Parsed message: {:?}", test_msg);
        }
    }
});

// Publish a message.
client.publish_structure("test/topic".to_string(), &test_message).await;

```

## API

### MqttierClient

#### `new(hostname: &str, port: u16, client_id: Option<String>) -> Result<Self>`

Creates a new MQTT client.

- `hostname`: The MQTT broker hostname
- `port`: The MQTT broker port
- `client_id`: Optional client ID. If `None`, a random UUID is generated

#### `run_loop() -> Result<()>`

Starts the connection loop. This should be called once per client. If already running, this method does nothing.

#### `subscribe(topic: String, qos: QoS, message_tx: mpsc::UnboundedSender<ReceivedMessage>) -> Result<u64>`

Subscribes to a topic and returns a subscription ID.

- `topic`: The MQTT topic to subscribe to
- `qos`: The Quality of Service level
- `message_tx`: The sender channel for delivering received messages
- Returns: The subscription ID (`u64`)


#### `publish_structure<T: Serialize>(topic: String, payload: T) -> Result<()>`

Publishes a serializable struct as JSON to a topic with QoS 2.

- `topic`: The MQTT topic to publish to
- `payload`: The struct to publish (must implement `Serialize`)

#### `publish_request<T: Serialize>(topic: String, payload: T, response_topic: String, correlation_id: Vec<u8>) -> Result<()>`

Publishes a request message with response topic and correlation ID.

- `topic`: The MQTT topic to publish to
- `payload`: The struct to publish
- `response_topic`: The topic for responses
- `correlation_id`: Correlation ID for matching responses (`Vec<u8>`)

#### `publish_response<T: Serialize>(topic: String, payload: T, correlation_id: Vec<u8>) -> Result<()>`

Publishes a response message with correlation ID.

- `topic`: The MQTT topic to publish to
- `payload`: The struct to publish
- `correlation_id`: Correlation ID for matching requests

#### `publish_state<T: Serialize>(topic: String, payload: T, state_version: u32) -> Result<()>`

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

The library uses the `log` crate. Initialize logging in your application:

```rust
env_logger::init();
```

## Example

See `examples/basic_usage.rs` for a complete example.

Run the example with:

```bash
cargo run --example basic_usage
```

## License

This project is licensed under the LGPLv2 License. (Not v3!)

# Metrics Feature

The metrics feature provides comprehensive tracking of MQTT operations in MQTTier. When enabled, the client collects detailed statistics about connections, message flow, performance, and reliability.

## Enabling Metrics

Add the `metrics` feature to your `Cargo.toml`:

```toml
[dependencies]
mqttier = { version = "0.6", features = ["metrics"] }
```

## Available Metrics

The metrics system tracks the following categories:

### Connection Metrics
- Connection attempts
- Successful connections
- Failed connections
- Reconnection count
- Current connection state
- Connection uptime
- Last connection duration

### Message Metrics (Published)
- Messages published by QoS level (0, 1, 2)
- Publish failures
- Total bytes sent
- Average message size

### Message Metrics (Received)
- Messages received by QoS level (0, 1, 2)
- Total bytes received
- Average message size

### Performance Metrics
- Pending publish count
- Average publish latency
- Maximum publish latency
- Minimum publish latency

### Reliability Metrics
- PUBACK acknowledgments received
- PUBCOMP completions received
- Publish timeouts
- Last error timestamp
- Publish success rate

### Subscription Metrics
- Active subscriptions
- Subscription requests
- Subscription failures

## Usage Example

```rust
use mqttier::{MqttierClient, MqttierOptions, Connection};
use stinger_mqtt_trait::MqttClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = MqttierOptions {
        connection: Connection::TcpLocalhost(1883),
        client_id: "metrics_client".to_string(),
        ack_timeout_ms: 5000,
        keepalive_secs: 60,
    };

    let mut client = MqttierClient::new(options)?;
    client.run_loop().await?;

    // Perform some MQTT operations...
    // (subscribe, publish, etc.)

    // Get metrics snapshot
    #[cfg(feature = "metrics")]
    {
        let metrics = client.get_metrics();
        
        println!("Connection attempts: {}", metrics.connection_attempts);
        println!("Messages published: {}", metrics.total_messages_published());
        println!("Messages received: {}", metrics.total_messages_received());
        println!("Average latency: {} ms", metrics.avg_publish_latency_ms);
        
        if let Some(uptime) = metrics.connection_uptime_ms() {
            println!("Uptime: {} ms", uptime);
        }
        
        if let Some(success_rate) = metrics.publish_success_rate() {
            println!("Publish success rate: {:.2}%", success_rate * 100.0);
        }
    }

    Ok(())
}
```

## MetricsSnapshot API

The `MetricsSnapshot` struct provides helper methods for computed metrics:

- `total_messages_published()` - Sum of all published messages across QoS levels
- `total_messages_received()` - Sum of all received messages across QoS levels
- `connection_uptime_ms()` - Current connection uptime in milliseconds (if connected)
- `avg_sent_message_size()` - Average size of sent messages in bytes
- `avg_received_message_size()` - Average size of received messages in bytes
- `publish_success_rate()` - Ratio of successful publishes (0.0 to 1.0)

## Resetting Metrics

You can reset all metrics to zero:

```rust
#[cfg(feature = "metrics")]
client.reset_metrics();
```

## Performance Considerations

The metrics implementation uses atomic operations for thread-safe updates with minimal overhead. All counters are lock-free and designed to have negligible impact on performance.

When the `metrics` feature is disabled, there is zero runtime overhead as all metric tracking code is conditionally compiled out.

## Example

Run the included example to see metrics in action:

```bash
cargo run --example metrics_demo --features metrics
```

This will connect to a local MQTT broker (localhost:1883), perform various operations, and display comprehensive metrics.

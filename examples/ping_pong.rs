//! # Ping Pong Example
//!
//! This example demonstrates:
//! - Connecting to a local MQTT broker at localhost:1883
//! - Publishing to the `example/ping` topic
//! - Subscribing to the `example/pong` topic
//! - Using Last Will and Testament (LWT) for connection status
//! - Publishing to `client/{client_id}/online` when connecting
//! - Using LWT to publish offline status on unexpected disconnect
//!
//! ## Prerequisites
//!
//! You need a MQTT broker running on localhost:1883. You can use Mosquitto:
//!
//! ```bash
//! # Install mosquitto (Ubuntu/Debian)
//! sudo apt-get install mosquitto mosquitto-clients
//!
//! # Start mosquitto
//! mosquitto -v
//! ```
//!
//! ## Running the Example
//!
//! ```bash
//! cargo run --example ping_pong
//! ```
//!
//! ## Testing with MQTT Client
//!
//! In another terminal, you can subscribe to the topics and respond:
//!
//! ```bash
//! # Subscribe to ping messages
//! mosquitto_sub -t "example/ping" -v
//!
//! # Subscribe to online status
//! mosquitto_sub -t "client/+/online" -v
//!
//! # Respond with pong messages
//! mosquitto_pub -t "example/pong" -m '{"pong":"Hello from mosquitto!"}'
//! ```

use bytes::Bytes;
use mqttier::{Connection, MqttierClient, MqttierOptions};
use std::collections::HashMap;
use std::time::Duration;
use stinger_mqtt_trait::message::{MqttMessage, MqttMessageBuilder, QoS};
use stinger_mqtt_trait::Mqtt5PubSub;
use tokio::sync::broadcast;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to stdout with thread IDs and targets
    tracing_subscriber::fmt()
        .with_writer(std::io::stdout)
        .with_thread_ids(true) // Show thread ID
        .with_target(true) // Show module target
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::DEBUG.into()),
        )
        .init();

    // Create a new MQTT client
    let options = MqttierOptions {
        connection: Connection::TcpLocalhost(1883),
        client_id: "ping_pong_client".to_string(),
        ack_timeout_ms: 5000,
        keepalive_secs: 60,
        session_expiry_interval_secs: 1200,
        availability_helper: Some(
            stinger_mqtt_trait::availability::AvailabilityHelper::client_availability(
                "local".to_string(),
                "ping_pong".to_string(),
            ),
        ),
        publish_queue_size: 128,
    };

    let mut client = MqttierClient::new(options)?;

    // Create a broadcast channel for receiving pong messages
    let (pong_tx, mut pong_rx) = broadcast::channel::<MqttMessage>(32);

    // Subscribe to 'example/pong' topic
    let subscription_result = client
        .subscribe("example/pong".to_string(), 1, pong_tx)
        .await?;
    println!("Subscribed to 'example/pong': {:?}", subscription_result);

    // Start the MQTT client event loop
    println!("Starting MQTT client...");
    client.start().await?;

    // Spawn a task to handle incoming pong messages
    println!("Spawning pong handler ...");
    let pong_handler = tokio::spawn(tracing::info_span!("pong_handler").in_scope(|| async move {
        while let Ok(message) = pong_rx.recv().await {
            let payload_str = String::from_utf8_lossy(&message.payload);
            println!("🏓 Received PONG: {}", payload_str);
        }
    }));

    // Spawn a task to publish ping messages
    println!("Spawning ping publisher.  Will publish ping message very 3 seconds (10 messages total) ...");
    let mut ping_client = client.clone();
    let pubtask = tokio::spawn(
        tracing::info_span!("ping_publisher").in_scope(|| async move {
            for i in 1..=10 {
                let ping_message_payload = format!(
                    "{{\"ping\":{},\"timestamp\":{}}}",
                    i,
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                );

                println!("🏓 Sending PING #{}", i);

                let ping_message = MqttMessageBuilder::default()
                    .topic("example/ping")
                    .payload(Bytes::from(ping_message_payload))
                    .qos(QoS::AtLeastOnce)
                    .retain(false)
                    .user_properties(HashMap::new())
                    .build()
                    .unwrap();

                match ping_client.publish(ping_message).await {
                    Ok(result) => println!("   PING #{} acknowledged: {:?}", i, result),
                    Err(e) => println!("   PING #{} failed: {:?}", i, e),
                }

                // Wait before sending next ping
                sleep(Duration::from_secs(3)).await;
            }
        }),
    );

    // Wait for the ping task to complete
    pubtask.await?;

    // Give some time for any remaining pongs to arrive
    sleep(Duration::from_secs(2)).await;

    // Cancel the pong handler
    pong_handler.abort();

    println!("Example completed!");

    #[cfg(feature = "metrics")]
    {
        let metrics = client.get_metrics();
        println!("\n=== Final Metrics ===");

        // Connection metrics
        println!("\n📡 Connection:");
        println!("  Connection attempts: {}", metrics.connection_attempts);
        println!(
            "  Successful connections: {}",
            metrics.successful_connections
        );
        println!("  Failed connections: {}", metrics.failed_connections);
        println!("  Reconnection count: {}", metrics.reconnection_count);
        println!("  Is connected: {}", metrics.is_connected);
        if let Some(uptime) = metrics.connection_uptime_ms() {
            println!(
                "  Uptime: {} ms ({:.2} seconds)",
                uptime,
                uptime as f64 / 1000.0
            );
        }

        // Publish metrics
        println!("\n📤 Publishing:");
        println!("  Total published: {}", metrics.total_messages_published());
        println!("    - QoS 0: {}", metrics.messages_published_qos0);
        println!("    - QoS 1: {}", metrics.messages_published_qos1);
        println!("    - QoS 2: {}", metrics.messages_published_qos2);
        println!("  Failed: {}", metrics.publish_failures);
        println!("  Timeouts: {}", metrics.publish_timeouts);
        if let Some(rate) = metrics.publish_success_rate() {
            println!("  Success rate: {:.2}%", rate * 100.0);
        }
        println!("  Total bytes sent: {}", metrics.total_bytes_sent);
        if let Some(avg_size) = metrics.avg_sent_message_size() {
            println!("  Avg message size: {} bytes", avg_size);
        }

        // Receive metrics
        println!("\n📥 Receiving:");
        println!("  Total received: {}", metrics.total_messages_received());
        println!("    - QoS 0: {}", metrics.messages_received_qos0);
        println!("    - QoS 1: {}", metrics.messages_received_qos1);
        println!("    - QoS 2: {}", metrics.messages_received_qos2);
        println!("  Total bytes received: {}", metrics.total_bytes_received);
        if let Some(avg_size) = metrics.avg_received_message_size() {
            println!("  Avg message size: {} bytes", avg_size);
        }

        // Subscription metrics
        println!("\n� Subscriptions:");
        println!("  Active subscriptions: {}", metrics.active_subscriptions);
        println!("  Subscription requests: {}", metrics.subscription_requests);
        println!("  Subscription failures: {}", metrics.subscription_failures);

        // Reliability metrics
        println!("\n✅ Reliability:");
        println!("  PUBACK received: {}", metrics.puback_received);
        println!("  PUBCOMP received: {}", metrics.pubcomp_received);

        // Performance metrics
        println!("\n⚡ Performance:");
        println!(
            "  Avg publish latency: {} ms",
            metrics.avg_publish_latency_us
        );
        println!(
            "  Min publish latency: {} us",
            metrics.min_publish_latency_us
        );
        println!(
            "  Max publish latency: {} us",
            metrics.max_publish_latency_us
        );
    }

    Ok(())
}

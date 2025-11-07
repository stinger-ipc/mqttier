//! Example demonstrating the metrics feature of mqttier
//!
//! This example shows how to use the metrics API to track MQTT operations.
//! To run this example with metrics enabled:
//! ```
//! cargo run --example metrics_demo --features metrics
//! ```

use mqttier::{Connection, MqttierClient, MqttierOptions};
use stinger_mqtt_trait::Mqtt5PubSub;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create client options
    let options = MqttierOptions {
        connection: Connection::TcpLocalhost(1883),
        client_id: "metrics_demo_client".to_string(),
        ack_timeout_ms: 5000,
        keepalive_secs: 60,
        session_expiry_interval_secs: 1200,
        availability_helper: Some(
            stinger_mqtt_trait::availability::AvailabilityHelper::client_availability(
                "local".to_string(),
                "metrics_demo".to_string(),
            ),
        ),
        publish_queue_size: 128,
    };

    // Create the MQTT client
    let mut client = MqttierClient::new(options)?;

    println!("Starting MQTT client with metrics enabled...");

    // Start the client
    client.run_loop().await?;

    // Wait for connection
    println!("Waiting for connection...");
    sleep(Duration::from_secs(2)).await;

    #[cfg(feature = "metrics")]
    {
        // Get initial metrics
        let initial_metrics = client.get_metrics();
        println!("\n=== Initial Metrics ===");
        println!(
            "Connection attempts: {}",
            initial_metrics.connection_attempts
        );
        println!(
            "Successful connections: {}",
            initial_metrics.successful_connections
        );
        println!("Is connected: {}", initial_metrics.is_connected);

        // Perform some operations
        println!("\n=== Performing MQTT operations ===");

        // Subscribe to a topic
        use tokio::sync::broadcast;
        let (tx, _rx) = broadcast::channel(100);
        match client.subscribe("test/metrics".to_string(), 1, tx).await {
            Ok(sub_id) => println!("Subscribed to test/metrics with ID: {}", sub_id),
            Err(e) => println!("Subscription failed: {}", e),
        }

        sleep(Duration::from_millis(500)).await;

        // Publish some messages
        use stinger_mqtt_trait::message::{MqttMessageBuilder, QoS};

        for i in 0..500 {
            let message = MqttMessageBuilder::default()
                .topic("test/metrics")
                .payload(format!("Message {}", i).into_bytes())
                .qos(QoS::AtLeastOnce)
                .retain(false)
                .build()
                .unwrap();

            match client.publish(message).await {
                Ok(_) => println!("Published message {}", i),
                Err(e) => println!("Publish failed: {}", e),
            }

            sleep(Duration::from_millis(100)).await;
        }

        // Wait for operations to complete
        sleep(Duration::from_secs(1)).await;

        // Get final metrics
        let final_metrics = client.get_metrics();

        println!("\n=== Final Metrics ===");
        println!("\n--- Connection Metrics ---");
        println!("Connection attempts: {}", final_metrics.connection_attempts);
        println!(
            "Successful connections: {}",
            final_metrics.successful_connections
        );
        println!("Failed connections: {}", final_metrics.failed_connections);
        println!("Reconnection count: {}", final_metrics.reconnection_count);
        println!("Currently connected: {}", final_metrics.is_connected);
        if let Some(uptime) = final_metrics.connection_uptime_ms() {
            println!("Connection uptime: {} ms", uptime);
        }

        println!("\n--- Message Metrics (Published) ---");
        println!("QoS 0 messages: {}", final_metrics.messages_published_qos0);
        println!("QoS 1 messages: {}", final_metrics.messages_published_qos1);
        println!("QoS 2 messages: {}", final_metrics.messages_published_qos2);
        println!(
            "Total published: {}",
            final_metrics.total_messages_published()
        );
        println!("Publish failures: {}", final_metrics.publish_failures);
        println!("Total bytes sent: {}", final_metrics.total_bytes_sent);
        if let Some(avg_size) = final_metrics.avg_sent_message_size() {
            println!("Average message size: {} bytes", avg_size);
        }

        println!("\n--- Message Metrics (Received) ---");
        println!("QoS 0 messages: {}", final_metrics.messages_received_qos0);
        println!("QoS 1 messages: {}", final_metrics.messages_received_qos1);
        println!("QoS 2 messages: {}", final_metrics.messages_received_qos2);
        println!(
            "Total received: {}",
            final_metrics.total_messages_received()
        );
        println!(
            "Total bytes received: {}",
            final_metrics.total_bytes_received
        );

        println!("\n--- Performance Metrics ---");
        println!(
            "Average publish latency: {} us",
            final_metrics.avg_publish_latency_us
        );
        println!(
            "Max publish latency: {} us",
            final_metrics.max_publish_latency_us
        );
        println!(
            "Min publish latency: {} us",
            final_metrics.min_publish_latency_us
        );

        println!("\n--- Reliability Metrics ---");
        println!("PUBACK received: {}", final_metrics.puback_received);
        println!("PUBCOMP received: {}", final_metrics.pubcomp_received);
        println!("Publish timeouts: {}", final_metrics.publish_timeouts);
        if let Some(success_rate) = final_metrics.publish_success_rate() {
            println!("Publish success rate: {:.2}%", success_rate * 100.0);
        }

        println!("\n--- Subscription Metrics ---");
        println!(
            "Active subscriptions: {}",
            final_metrics.active_subscriptions
        );
        println!(
            "Subscription requests: {}",
            final_metrics.subscription_requests
        );
        println!(
            "Subscription failures: {}",
            final_metrics.subscription_failures
        );

        // Demonstrate metrics reset
        println!("\n=== Resetting metrics ===");
        client.reset_metrics();
        let reset_metrics = client.get_metrics();
        println!(
            "Messages published after reset: {}",
            reset_metrics.total_messages_published()
        );
    }

    #[cfg(not(feature = "metrics"))]
    {
        println!("\nMetrics feature is not enabled!");
        println!("Run this example with: cargo run --example metrics_demo --features metrics");
    }

    // Clean shutdown
    client.clean_stop().await?;
    println!("\nClient stopped.");

    Ok(())
}

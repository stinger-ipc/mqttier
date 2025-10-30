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

use mqttier::{MqttierClient, MqttierOptions, Connection};
use stinger_mqtt_trait::Mqtt5PubSub;
use stinger_mqtt_trait::message::{MqttMessage, MqttMessageBuilder, QoS};
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to stdout
    tracing_subscriber::fmt()
        .with_writer(std::io::stdout)
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
    };
    
    let mut client = MqttierClient::new(options)?;
    
    // Set up Last Will and Testament (LWT)
    // This message will be published by the broker if the client disconnects unexpectedly
    let client_id = client.get_client_id();
    let lwt_topic = format!("client/{}/online", client_id);
    let lwt_message = MqttMessageBuilder::default()
        .topic(&lwt_topic)
        .payload(Bytes::from("{\"state\":\"offline\"}"))
        .qos(QoS::AtLeastOnce)
        .retain(true)
        .content_type(Some("application/json".to_string()))
        .user_properties(HashMap::new())
        .build()
        .unwrap();
    client.set_last_will(lwt_message);

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

    // Wait a moment for connection to establish
    sleep(Duration::from_millis(500)).await;


    println!("Publishing online status...");
    // Publish the "online" message to the same topic as the LWT
    let online_message = MqttMessageBuilder::default()
        .topic(&lwt_topic)
        .payload(Bytes::from("{\"state\":\"online\"}"))
        .qos(QoS::AtLeastOnce)
        .retain(true)
        .content_type(Some("application/json".to_string()))
        .user_properties(HashMap::new())
        .build()
        .unwrap();

    match client.publish(online_message).await {
        Ok(result) => println!("Published online status: {:?}", result),
        Err(e) => println!("Failed to publish online status: {:?}", e),
    }

    // Spawn a task to handle incoming pong messages
    println!("Spawning pong handler ...");
    let pong_handler = tokio::spawn(async move {
        while let Ok(message) = pong_rx.recv().await {
            let payload_str = String::from_utf8_lossy(&message.payload);
            println!("🏓 Received PONG: {}", payload_str);
        }
    });

    // Spawn a task to publish ping messages
    println!("Spawning ping publisher.  Will publish ping message very 3 seconds (10 messages total) ...");
    let mut ping_client = client.clone();
    let pubtask = tokio::spawn(async move {
        for i in 1..=10 {
            let ping_message_payload = format!("{{\"ping\":{},\"timestamp\":{}}}", 
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
    });

    // Wait for the ping task to complete
    pubtask.await?;

    // Give some time for any remaining pongs to arrive
    sleep(Duration::from_secs(2)).await;

    // Publish offline status before exiting (graceful shutdown)
    let offline_message = MqttMessageBuilder::default()
        .topic(&lwt_topic)
        .payload(Bytes::from("{\"state\":\"offline\"}"))
        .qos(QoS::AtLeastOnce)
        .retain(true)
        .content_type(Some("application/json".to_string()))
        .user_properties(HashMap::new())
        .build()
        .unwrap();

    match client.publish(offline_message).await {
        Ok(result) => println!("Published offline status: {:?}", result),
        Err(e) => println!("Failed to publish offline status: {:?}", e),
    }

    // Cancel the pong handler
    pong_handler.abort();

    println!("Example completed!");

    Ok(())
}

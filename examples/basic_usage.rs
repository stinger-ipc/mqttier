use mqttier::{MqttierClient, ReceivedMessage};
use rumqttc::v5::mqttbytes::QoS;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    id: u32,
    content: String,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Create a new MQTT client
    let client = MqttierClient::new("localhost", 1883, Some("mqttier_example".to_string()))?;

    // Start the run loop
    client.run_loop().await?;

    // Wait a bit for connection
    sleep(Duration::from_secs(2)).await;

    // Create mpsc channel for receiving messages
    let (message_tx, mut message_rx) = mpsc::unbounded_channel::<ReceivedMessage>();

    // Subscribe to a topic
    let subscription_id = client.subscribe("test/topic".to_string(), QoS::AtMostOnce, message_tx).await?;
    println!("Subscribed to 'test/topic' with ID: {}", subscription_id);

    // Start a task to handle incoming messages
    tokio::spawn(async move {
        while let Some(message) = message_rx.recv().await {
            println!("Received message: {:?}", message.topic);
            
            // Try to deserialize as our test message
            if let Ok(test_msg) = serde_json::from_slice::<TestMessage>(&message.payload) {
                println!("Parsed message: {:?}", test_msg);
            }
        }
    });

    // Publish some test messages
    for i in 0..5 {
        let test_message = TestMessage {
            id: i,
            content: format!("Hello, MQTT! Message #{}", i),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        client.publish_structure("test/topic".to_string(), &test_message).await?;
        println!("Published message #{}", i);

        sleep(Duration::from_secs(1)).await;
    }

    // Keep the program running for a bit to see the messages
    sleep(Duration::from_secs(10)).await;

    Ok(())
}

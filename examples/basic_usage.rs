use mqttier::{MqttierClient, ReceivedMessage};
use serde::{Deserialize, Serialize};
use std::fmt::format;
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
    let (message_tx, mut message_rx) = mpsc::channel::<ReceivedMessage>(64);

    // Subscribe to a topic
    let subscription_id = client.subscribe("test/topic".to_string(), 0, message_tx).await?;
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
    let client2 = client.clone();
    tokio::spawn(async move {
        for i in 0..10 {
            let test_message = TestMessage {
                id: i,
                content: format!("Hello, MQTT! Message #{}", i),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };

            let _ = client2.publish_structure("test/topic".to_string(), &test_message).await;
            println!("Published message #{}", i);

            sleep(Duration::from_secs(1)).await;
        }
    });

    // Publish some test messages
    let client3 = client.clone();
    tokio::spawn(async move {
        for i in 0..5 {
            let _ = client3.publish_string("test/hello".to_string(), format!("Hello World {}", i), 1, true, None).await;
            println!("Published hello message #{}", i);

            sleep(Duration::from_secs(2)).await;
        }
    });

    let client4 = client.clone();
    tokio::spawn(async move {
        for i in 10..15 {
            let _ = client4.publish_state("test/state".to_string(), vec![i; 10], 1).await;
            println!("Published state message #{}", i);
        }
        sleep(Duration::from_secs(3)).await;
    });

    // Keep the program running for a bit to see the messages
    sleep(Duration::from_secs(10)).await;

    Ok(())
}

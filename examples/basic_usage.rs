use mqttier::{MqttierClient, ReceivedMessage};
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
    // Initialize tracing to stdout
    tracing_subscriber::fmt()
        .with_writer(std::io::stdout)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Create a new MQTT client
    let client = MqttierClient::new("localhost", 1883, Some("mqttc".to_string()))?;

    // Create mpsc channel for receiving messages
    let (message_tx, mut message_rx) = mpsc::channel::<ReceivedMessage>(64);

    // Subscribe to a topic
    let subscription_id = client.subscribe("test/command".to_string(), 0, message_tx).await?;
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

    // Start the MQTT client loop
    println!("Starting MQTT client loop...");
    client.run_loop().await?;

    let client1 = client.clone();
    let pub_task = tokio::spawn(async move {
        for i in 0..6 {
            let published_oneshot = client1.publish_string("test/string".to_string(), format!("Hello, MQTT! Message #{}", i), 1, false, None).await.unwrap();
            
            // Wait for acknowledgment
            let start = std::time::Instant::now();
            match published_oneshot.await {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    println!(
                        "Message QOS=1 #{} acknowledged: {:?} (waited {:?})",
                        i, result, elapsed
                    );
                }
                Err(_) => println!("Message #{} completion channel closed", i),
            }
            sleep(Duration::from_secs(3)).await;
        }
    });

    let client11 = client.clone();
    let pub_task = tokio::spawn(async move {
        for i in 30..36 {
            let published_oneshot = client11.publish_string("test/string".to_string(), format!("Hello, MQTT! Message #{}", i), 2, false, None).await.unwrap();
            
            // Wait for acknowledgment
            let start = std::time::Instant::now();
            match published_oneshot.await {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    println!(
                        "Message QOS=2 #{} completed: {:?} (waited {:?})",
                        i, result, elapsed
                    );
                }
                Err(_) => println!("Message #{} completion channel closed", i),
            }
            sleep(Duration::from_secs(4)).await;
        }
    });

    let client111 = client.clone();
    let pub_task = tokio::spawn(async move {
        for i in 40..46 {
            let published_oneshot = client111.publish_string("test/string".to_string(), format!("Hello, MQTT! Message #{}", i), 0, false, None).await.unwrap();

            // Wait for acknowledgment
            let start = std::time::Instant::now();
            match published_oneshot.await {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    println!(
                        "Message QOS=0 #{} acknowledged: {:?} (waited {:?})",
                        i, result, elapsed
                    );
                }
                Err(_) => println!("Message #{} completion channel closed", i),
            }
            sleep(Duration::from_secs(2)).await;
        }
    });

    let client2 = client.clone();
    let pub_task2 = tokio::spawn(async move {
        for i in 19..25 {
            let test_message = TestMessage {
                id: i,
                content: format!("Hello from another task #{}", i),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };

            let publish_rx = client2.publish_structure("test/structure".to_string(), &test_message).await.unwrap();
            println!("Published structure message #{}", i);
            
            // Wait for acknowledgment
            match publish_rx.await {
                Ok(result) => println!("Structure message #{} acknowledged: {:?}", i, result),
                Err(_) => println!("Structure message #{} completion channel closed", i),
            }

            sleep(Duration::from_secs(1)).await;
        }
    });

    // Publish some test messages
    let client3 = client.clone();
    tokio::spawn(async move {
        for i in 0..9 {
            let _ = client3.publish_string("test/hello".to_string(), format!("Hello World {}", i), 0, true, None).await;
            println!("Published qos=0 hello message #{}", i);

            sleep(Duration::from_secs(2)).await;
        }
    });

    let client4 = client.clone();
    tokio::spawn(async move {
        for i in 10..18 {
            let _ = client4.publish_state("test/state".to_string(), vec![i; 10], 1).await;
            println!("Published state message #{}", i);
            sleep(Duration::from_secs(3)).await;
        }
    });


    // Keep the program running for a bit to see the messages
    sleep(Duration::from_secs(25)).await;

    pub_task.await?;
    pub_task2.await?;

    Ok(())
}

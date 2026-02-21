use bytes::Bytes;
use mqttier::{Connection, MqttierClient, MqttierOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use stinger_mqtt_trait::message::{MqttMessage, MqttMessageBuilder, QoS};
use stinger_mqtt_trait::Mqtt5PubSub;
use tokio::sync::broadcast;
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

    // Create a new MQTT client using `MqttierOptions`
    let options = MqttierOptions {
        connection: Connection::TcpLocalhost(1883),
        client_id: "mqttc".to_string(),
        ack_timeout_ms: 5000,
        keepalive_secs: 60,
        session_expiry_interval_secs: 1200,
        availability_helper: Some(
            stinger_mqtt_trait::availability::AvailabilityHelper::client_availability(
                "local".to_string(),
                "basic_usage".to_string(),
            ),
        ),
        publish_queue_size: 128,
        max_incoming_packet_size: 10 * 1024,
        credentials: None,
    };
    let mut client = MqttierClient::new(options)?;

    // Create broadcast channel for receiving messages
    let (message_tx, mut message_rx) = broadcast::channel::<MqttMessage>(64);

    // Subscribe to a topic
    let subscription_id = client
        .subscribe("test/command".to_string(), 0, message_tx)
        .await?;
    println!("Subscribed to 'test/command' with ID: {}", subscription_id);

    // Start a task to handle incoming messages
    tokio::spawn(async move {
        while let Ok(message) = message_rx.recv().await {
            println!("Received message: {:?}", message.topic);

            // Try to deserialize as our test message
            if let Ok(test_msg) = serde_json::from_slice::<TestMessage>(&message.payload) {
                println!("Parsed message: {:?}", test_msg);
            }
        }
    });

    // Start the MQTT client loop
    println!("Starting MQTT client loop...");
    client.start().await?;

    let mut client1 = client.clone();
    let _pub_task = tokio::spawn(async move {
        for i in 0..6 {
            let message = MqttMessageBuilder::default()
                .topic("test/string")
                .payload(Bytes::from(format!("Hello, MQTT! Message #{}", i)))
                .qos(QoS::AtLeastOnce)
                .retain(false)
                .user_properties(HashMap::new())
                .build()
                .unwrap();

            // Wait for acknowledgment
            let start = std::time::Instant::now();
            match client1.publish(message).await {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    println!(
                        "Message QOS=1 #{} acknowledged: {:?} (waited {:?})",
                        i, result, elapsed
                    );
                }
                Err(e) => println!("Message #{} failed: {:?}", i, e),
            }
            sleep(Duration::from_secs(3)).await;
        }
    });

    let mut client11 = client.clone();
    let _pub_task = tokio::spawn(async move {
        for i in 30..36 {
            let message = MqttMessageBuilder::default()
                .topic("test/string")
                .payload(Bytes::from(format!("Hello, MQTT! Message #{}", i)))
                .qos(QoS::ExactlyOnce)
                .retain(false)
                .user_properties(HashMap::new())
                .build()
                .unwrap();

            // Wait for acknowledgment
            let start = std::time::Instant::now();
            match client11.publish(message).await {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    println!(
                        "Message QOS=2 #{} completed: {:?} (waited {:?})",
                        i, result, elapsed
                    );
                }
                Err(e) => println!("Message #{} failed: {:?}", i, e),
            }
            sleep(Duration::from_secs(4)).await;
        }
    });

    let mut client111 = client.clone();
    let pub_task = tokio::spawn(async move {
        for i in 40..46 {
            let message = MqttMessageBuilder::default()
                .topic("test/string")
                .payload(Bytes::from(format!("Hello, MQTT! Message #{}", i)))
                .qos(QoS::AtMostOnce)
                .retain(false)
                .user_properties(HashMap::new())
                .build()
                .unwrap();

            // Wait for acknowledgment
            let start = std::time::Instant::now();
            match client111.publish(message).await {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    println!(
                        "Message QOS=0 #{} acknowledged: {:?} (waited {:?})",
                        i, result, elapsed
                    );
                }
                Err(e) => println!("Message #{} failed: {:?}", i, e),
            }
            sleep(Duration::from_secs(2)).await;
        }
    });

    let mut client2 = client.clone();
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

            let json_payload = serde_json::to_vec(&test_message).unwrap();
            let message = MqttMessageBuilder::default()
                .topic("test/structure")
                .payload(Bytes::from(json_payload))
                .qos(QoS::AtLeastOnce)
                .retain(false)
                .content_type(Some("application/json".to_string()))
                .user_properties(HashMap::new())
                .build()
                .unwrap();

            println!("Published structure message #{}", i);

            // Wait for acknowledgment
            match client2.publish(message).await {
                Ok(result) => println!("Structure message #{} acknowledged: {:?}", i, result),
                Err(e) => println!("Structure message #{} failed: {:?}", i, e),
            }

            sleep(Duration::from_secs(1)).await;
        }
    });

    // Publish some test messages
    let mut client3 = client.clone();
    tokio::spawn(async move {
        for i in 0..9 {
            let message = MqttMessageBuilder::default()
                .topic("test/hello")
                .payload(Bytes::from(format!("Hello World {}", i)))
                .qos(QoS::AtMostOnce)
                .retain(true)
                .user_properties(HashMap::new())
                .build()
                .unwrap();

            let _ = client3.publish(message).await;
            println!("Published qos=0 hello message #{}", i);

            sleep(Duration::from_secs(2)).await;
        }
    });

    let mut client4 = client.clone();
    tokio::spawn(async move {
        for i in 10..18 {
            let state_vec = vec![i; 10];
            let message = MqttMessageBuilder::default()
                .topic("test/state")
                .payload(Bytes::from(format!("{:?}", state_vec)))
                .qos(QoS::AtLeastOnce)
                .retain(false)
                .user_properties(HashMap::new())
                .build()
                .unwrap();

            let _ = client4.publish(message).await;
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

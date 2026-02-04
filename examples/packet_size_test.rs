use bytes::Bytes;
use mqttier::{Connection, MqttierClient, MqttierOptions};
use std::collections::HashMap;
use std::error::Error;
use stinger_mqtt_trait::message::{MqttMessageBuilder, QoS};
use stinger_mqtt_trait::Mqtt5PubSub;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to stdout
    tracing_subscriber::fmt()
        .with_writer(std::io::stdout)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    println!("=== MQTT Packet Size Test ===");
    println!("Testing maximum reliable packet size by incrementing payload by 1000 bytes\n");

    // Create a new MQTT client using `MqttierOptions`
    let options = MqttierOptions {
        connection: Connection::TcpLocalhost(1883),
        client_id: "packet_size_test".to_string(),
        ack_timeout_ms: 5000,
        keepalive_secs: 60,
        session_expiry_interval_secs: 1200,
        availability_helper: Some(
            stinger_mqtt_trait::availability::AvailabilityHelper::client_availability(
                "local".to_string(),
                "packet_size_test".to_string(),
            ),
        ),
        publish_queue_size: 128,
        max_incoming_packet_size: 2000,
    };
    let mut client = MqttierClient::new(options)?;

    // Start the MQTT client loop
    println!("Starting MQTT client...");
    client.start().await?;
    println!("Client started successfully\n");

    // Test incrementing packet sizes
    let mut size = 1000;
    let increment = 1000;
    let mut last_successful_size = 0;
    
    loop {
        // Create payload of the current size
        let payload_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let payload = Bytes::from(payload_data);
        
        println!("Testing packet size: {} bytes", size);
        
        let message = MqttMessageBuilder::default()
            .topic("test/packet_size")
            .payload(payload)
            .qos(QoS::AtLeastOnce)
            .retain(false)
            .user_properties(HashMap::new())
            .build()
            .unwrap();

        // Attempt to publish
        let start = std::time::Instant::now();
        match client.publish(message).await {
            Ok(result) => {
                let elapsed = start.elapsed();
                println!("✓ Success! Size: {} bytes - Acknowledged in {:?}", size, elapsed);
                println!("  Result: {:?}\n", result);
                last_successful_size = size;
            }
            Err(e) => {
                let elapsed = start.elapsed();
                println!("\n========================================");
                println!("✗ ERROR ENCOUNTERED!");
                println!("========================================");
                println!("Packet size: {} bytes", size);
                println!("Time elapsed: {:?}", elapsed);
                println!("Last successful size: {} bytes", last_successful_size);
                println!("\nError details:");
                println!("  Error: {:?}", e);
                println!("  Display: {}", e);
                println!("  Debug representation: {:#?}", e);
                
                // Try to extract source errors
                let mut source = e.source();
                let mut level = 1;
                while let Some(err) = source {
                    println!("\n  Source error level {}: {}", level, err);
                    println!("  Source error debug: {:?}", err);
                    source = err.source();
                    level += 1;
                }
                
                println!("\n========================================");
                println!("SUMMARY");
                println!("========================================");
                println!("Maximum successful packet size: {} bytes", last_successful_size);
                println!("Failed at packet size: {} bytes", size);
                println!("========================================\n");
                
                break;
            }
        }
        
        // Increment size for next iteration
        size += increment;
        
        // Optional: add a small delay between publishes to avoid overwhelming the broker
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }

    println!("Test completed.");
    Ok(())
}

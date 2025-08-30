//! # MQTTier
//! 
//! A Rust MQTT client library providing an abstracted interface around rumqttc.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use rumqttc::v5::{MqttOptions, AsyncClient, EventLoop, Event};
use rumqttc::v5::mqttbytes::v5::{PublishProperties, SubscribeProperties, Packet};
use rumqttc::v5::mqttbytes::{QoS};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;

/// Errors that can occur when using MqttierClient
#[derive(Error, Debug)]
pub enum MqttierError {
    #[error("MQTT connection error: {0}")]
    ConnectionError(#[from] rumqttc::v5::ConnectionError),
    #[error("MQTT client error: {0}")]
    ClientError(#[from] rumqttc::v5::ClientError),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Channel send error")]
    ChannelSendError,
    #[error("Invalid QoS value: {0}")]
    InvalidQos(u8),
}

type Result<T> = std::result::Result<T, MqttierError>;

/// Represents a queued subscription
#[derive(Debug, Clone)]
struct QueuedSubscription {
    topic: String,
    qos: QoS,
    props: SubscribeProperties,
}

/// Represents a queued message to publish
#[derive(Debug, Clone)]
struct QueuedMessage {
    topic: String,
    payload: Vec<u8>,
    qos: QoS,
    retain: bool,
    publish_props: PublishProperties,
}

/// A message received from a subscription
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub subscription_id: usize,
    pub response_topic: Option<String>,
    pub content_type: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
}

/// Internal state of the MqttierClient
#[derive(Debug)]
struct ClientState {
    is_connected: bool,
    subscriptions: HashMap<usize, mpsc::Sender<ReceivedMessage>>,
    queued_subscriptions: Vec<QueuedSubscription>,
    queued_messages: Vec<QueuedMessage>,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            is_connected: false,
            subscriptions: HashMap::new(),
            queued_subscriptions: Vec::new(),
            queued_messages: Vec::new(),
        }
    }
}

/// MqttierClient provides an abstracted interface around rumqttc
#[derive(Clone)]
pub struct MqttierClient {
    pub client_id: String,
    client: AsyncClient,
    state: Arc<RwLock<ClientState>>,
    next_subscription_id: Arc<AtomicUsize>,
    is_running: Arc<Mutex<bool>>,
    eventloop: Arc<Mutex<Option<EventLoop>>>,
}

impl MqttierClient {
    /// Create a new MqttierClient
    /// 
    /// # Arguments
    /// 
    /// * `hostname` - The hostname of the MQTT broker
    /// * `port` - The port of the MQTT broker
    /// * `client_id` - Optional client ID. If None, a random UUID will be generated
    pub fn new(hostname: &str, port: u16, client_id: Option<String>) -> Result<Self> {
        let client_id = client_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        
        let mut mqttoptions = MqttOptions::new(client_id.clone(), hostname, port);
        mqttoptions.set_keep_alive(Duration::from_secs(60));
        mqttoptions.set_clean_start(true);

        let (client, eventloop) = AsyncClient::new(mqttoptions, 10);

        Ok(Self {
            client_id,
            client,
            state: Arc::new(RwLock::new(ClientState::default())),
            next_subscription_id: Arc::new(AtomicUsize::new(5)),
            is_running: Arc::new(Mutex::new(false)),
            eventloop: Arc::new(Mutex::new(Some(eventloop))),
        })
    }

    /// Get the next subscription id value
    fn next_subscription_id(&self) -> usize {
        self.next_subscription_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Subscribe to a topic
    /// 
    /// # Arguments
    /// 
    /// * `topic` - The topic to subscribe to
    /// * `qos` - The QoS level for the subscription
    /// 
    /// # Returns
    /// 
    /// Returns a subscription ID and a receiver for messages on this topic
    pub async fn subscribe(&self, topic: String, qos: u8, received_message_tx: mpsc::Sender<ReceivedMessage>) -> Result<usize> {
        let subscription_id = self.next_subscription_id();

        let mut state = self.state.write().await;
        state.subscriptions.insert(subscription_id, received_message_tx);
        let subscription_props = SubscribeProperties {
            id: Some(subscription_id),
            user_properties: Vec::new(),
        };
        let rumqttc_qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => return Err(MqttierError::InvalidQos(qos)),
        };
        if state.is_connected {
            debug!("Subscribing to topic: {} with QoS: {:?}", topic, qos);
            self.client.subscribe_with_properties(&topic, rumqttc_qos, subscription_props).await?;
        } else {
            debug!("Queueing subscription for topic: {} with QoS: {:?}", topic, qos);
            state.queued_subscriptions.push(QueuedSubscription {
                topic,
                qos: rumqttc_qos,
                props: subscription_props,
            });
        }

        Ok(subscription_id)
    }

    /// Publish a message to a topic
    /// 
    /// # Arguments
    /// 
    /// * `topic` - The topic to publish to
    /// * `message` - The message to publish (must be serializable)
    /// * `qos` - The QoS level for the message
    pub async fn publish(&self, topic: String, payload: Vec<u8>, qos: QoS , retain: bool, publish_props: Option<PublishProperties>) -> Result<()> {
        let mut state = self.state.write().await;
        let publish_props = publish_props.unwrap_or_default();
        if state.is_connected {
            debug!("Publishing message to topic: {} with QoS: {:?}", topic, qos);
            self.client.publish_with_properties(&topic, qos, retain, payload, publish_props).await?;
        } else {
            debug!("Queueing message for topic: {} with QoS: {:?}", topic, qos);
            state.queued_messages.push(QueuedMessage {
                topic,
                payload,
                qos,
                retain,
                publish_props,
            });
        }

        Ok(())
    }

    /// Publish a string message to a topic
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The string payload to send
    /// * `qos` - The QoS level for the message (0, 1, or 2)
    /// * `retain` - Whether to retain the message
    /// * `publish_props` - Optional publish properties
    pub async fn publish_string(
        &self,
        topic: String,
        payload: String,
        qos: u8,
        retain: bool,
        publish_props: Option<PublishProperties>,
    ) -> Result<()> {
        let rumqttc_qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => return Err(MqttierError::InvalidQos(qos)),
        };
        self.publish(topic, payload.into_bytes(), rumqttc_qos, retain, publish_props).await
    }

    /// Publish a structure message to a topic
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The serializable struct to send as payload
    /// * `state_version` - The version of the structure
    pub async fn publish_structure<T: Serialize>(
        &self,
        topic: String,
        payload: T,
    ) -> Result<()> {
        let mut props = PublishProperties::default();
        props.content_type = Some("application/json".to_string());
        let payload_bytes = serde_json::to_vec(&payload)?;
        self.publish(topic, payload_bytes, QoS::ExactlyOnce, false, Some(props)).await
    }

    /// Publish a request message to a topic with response topic and correlation id
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The serializable struct to send as payload
    /// * `response_topic` - The topic where responses should be sent
    /// * `correlation_id` - The correlation id for matching responses
    pub async fn publish_request<T: Serialize>(
        &self,
        topic: String,
        payload: T,
        response_topic: String,
        correlation_id: Vec<u8>,
    ) -> Result<()> {
        let mut props = PublishProperties::default();
        props.response_topic = Some(response_topic);
        props.correlation_data = Some(correlation_id.into());
        props.content_type = Some("application/json".to_string());
        let payload_bytes = serde_json::to_vec(&payload)?;
        self.publish(topic, payload_bytes, QoS::ExactlyOnce, false, Some(props)).await
    }

    /// Publish a response message to a topic with correlation id
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The serializable struct to send as payload
    /// * `correlation_id` - The correlation id for matching requests
    pub async fn publish_response<T: Serialize>(
        &self,
        topic: String,
        payload: T,
        correlation_id: Vec<u8>,
    ) -> Result<()> {
        let mut props = PublishProperties::default();
        props.content_type = Some("application/json".to_string());
        props.correlation_data = Some(correlation_id.into());
        let payload_bytes = serde_json::to_vec(&payload)?;
        self.publish(topic, payload_bytes, QoS::ExactlyOnce, false, Some(props)).await
    }

    /// Publish a state message to a topic
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The serializable struct to send as payload
    pub async fn publish_state<T: Serialize>(
        &self,
        topic: String,
        payload: T,
        state_version: u32,
    ) -> Result<()> {
        let mut props = PublishProperties::default();
        props.user_properties.push((
            "PropertyVersion".to_string(),
            state_version.to_string(),
        ));
        props.content_type = Some("application/json".to_string());
        let payload_bytes = serde_json::to_vec(&payload)?;
        self.publish(topic, payload_bytes, QoS::AtLeastOnce, true, None).await
    }

    /// Start the run loop for handling MQTT connections and messages
    pub async fn run_loop(&self) -> Result<()> {
        let mut is_running = self.is_running.lock().await;
        if *is_running {
            debug!("Run loop is already running");
            return Ok(());
        }
        *is_running = true;
        drop(is_running);

        let client = self.client.clone();
        let state = self.state.clone();
        let eventloop = self.eventloop.clone();

        tokio::spawn(async move {
            loop {
                info!("Starting MQTT connection loop");
                
                // Take ownership of the eventloop
                let mut eventloop_guard = eventloop.lock().await;
                if let Some(mut el) = eventloop_guard.take() {
                    drop(eventloop_guard);
                    
                    match Self::handle_connection(&client, &mut el, &state).await {
                        Ok(_) => {
                            info!("MQTT connection loop ended normally");
                        }
                        Err(e) => {
                            error!("MQTT connection error: {}", e);
                        }
                    }
                } else {
                    error!("EventLoop not available");
                    break;
                }

                // Mark as disconnected
                {
                    let mut state_guard = state.write().await;
                    state_guard.is_connected = false;
                }

                // Wait before reconnecting
                warn!("Reconnecting in 5 seconds...");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        Ok(())
    }

    /// Handle a single MQTT connection
    async fn handle_connection(
        client: &AsyncClient,
        eventloop: &mut EventLoop,
        state: &Arc<RwLock<ClientState>>,
    ) -> Result<()> {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("Connected to MQTT broker");
                    
                    let mut state_guard = state.write().await;
                    state_guard.is_connected = true;

                    // Process queued subscriptions
                    for subscription in state_guard.queued_subscriptions.drain(..) {
                        debug!("Processing queued subscription for topic: {}", subscription.topic);
                        if let Err(e) = client.subscribe_with_properties(&subscription.topic, subscription.qos, subscription.props).await {
                            error!("Failed to subscribe to {}: {}", subscription.topic, e);
                        }
                    }

                    // Process queued messages
                    for message in state_guard.queued_messages.drain(..) {
                        debug!("Processing queued message for topic: {}", message.topic);
                        if let Err(e) = client.publish_with_properties(&message.topic, message.qos, message.retain, message.payload, message.publish_props).await {
                            error!("Failed to publish to {}: {}", message.topic, e);
                        }
                    }
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let topic_str = String::from_utf8_lossy(&publish.topic).to_string();
                    debug!("Received message on topic: {}", topic_str);

                    if let Some(pub_props) = publish.properties {
                        let subscription_ids = pub_props.subscription_identifiers;
                        let correlation_data = pub_props.correlation_data;
                        let response_topic = pub_props.response_topic;
                        let content_type = pub_props.content_type;
                        for subscription_id in subscription_ids {
                            let message = ReceivedMessage {
                                topic: topic_str.clone(),
                                payload: publish.payload.to_vec(),
                                qos: match publish.qos {
                                    QoS::AtMostOnce => 0,
                                    QoS::AtLeastOnce => 1,
                                    QoS::ExactlyOnce => 2,
                                },
                                subscription_id: subscription_id,
                                response_topic: response_topic.clone(),
                                correlation_data: correlation_data.clone().map(|data| data.to_vec()),
                                content_type: content_type.clone(),
                            };
                            let state_guard = state.read().await;
                            if let Some(sender) = state_guard.subscriptions.get(&subscription_id) {
                                if let Err(_) = sender.send(message.clone()).await {
                                    warn!("Failed to send message to subscription {}", subscription_id);
                                }
                            }
                        }
                    }
                }
                Ok(Event::Incoming(packet)) => {
                    debug!("Received packet: {:?}", packet);
                }
                Ok(Event::Outgoing(_)) => {
                    // Outgoing events don't need special handling
                }
                Err(e) => {
                    error!("Event loop error: {}", e);
                    return Err(MqttierError::ConnectionError(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        let client = MqttierClient::new("localhost", 1883, None).unwrap();
        assert_eq!(client.next_subscription_id.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_client_creation_with_id() {
        let client_id = "test_client".to_string();
        let _client = MqttierClient::new("localhost", 1883, Some(client_id)).unwrap();
        // If we get here without panic, the test passes
    }
}

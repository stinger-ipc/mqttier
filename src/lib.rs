//! # MQTTier
//! 
//! A Rust MQTT client library providing an abstracted interface around rumqttc.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error, info, warn};
use rumqttc::v5::{MqttOptions, AsyncClient, EventLoop, Event};
use rumqttc::v5::mqttbytes::v5::{PublishProperties, SubscribeProperties, Packet};
use rumqttc::v5::mqttbytes::{QoS};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
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

/// Result of a publish operation indicating when the message lifecycle completed.
///
/// Variants:
/// - `Acknowledged(pkid)` - QoS 1: broker acknowledged the publish (PUBACK)
/// - `Completed(pkid)` - QoS 2: publish flow completed (PUBCOMP)
/// - `Sent(pkid)` - QoS 0: message was sent but no broker ack expected
/// - `TimedOut` - waiting for the send packet id timed out
/// - `Error(..)` - internal or channel errors
#[derive(Debug)]
pub enum PublishResult {
    /// Message was acknowledged by broker (QoS 1 PUBACK)
    Acknowledged(u16),

    /// Message transmission to broker was completed (QoS 2 PUBCOMP)
    Completed(u16),

    /// No acknowledgment expected (QoS 0)
    Sent(u16),

    /// Timed Out
    TimedOut,

    // Serialization Failed
    SerializationError(String),

    /// Internal Error
    Error(String),
}

/// Completion signal for a published message
type PublishCompletion = oneshot::Sender<PublishResult>;

/// Represents a publish waiting for acknowledgment
#[derive(Debug)]
struct PendingPublish {
    qos: QoS,
    completion: PublishCompletion,
}

/// Represents a queued subscription
#[derive(Debug, Clone)]
struct QueuedSubscription {
    topic: String,
    qos: QoS,
    props: SubscribeProperties,
}

/// Represents a queued message to publish
#[derive(Debug)]
struct QueuedMessage {
    topic: String,
    payload: Vec<u8>,
    qos: QoS,
    retain: bool,
    publish_props: PublishProperties,
    completion: Option<PublishCompletion>,
}

/// A message received from a subscription.
///
/// This is delivered to subscribers via the `mpsc::Sender<ReceivedMessage>` registered
/// when calling `MqttierClient::subscribe`.
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub subscription_id: usize,
    pub response_topic: Option<String>,
    pub content_type: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    /// User properties from the MQTT5 publish properties. Keys and values are strings.
    pub user_properties: HashMap<String, String>,
}

/// Internal state of the MqttierClient
#[derive(Debug)]
struct ClientState {
    is_connected: bool,
    subscriptions: HashMap<usize, mpsc::Sender<ReceivedMessage>>,
    queued_subscriptions: Vec<QueuedSubscription>,
    /// This is a map to PendingPublish structs keyed by packet ID. When we get an PUBACK or PUBCOMP, we look up the packet ID here to find the completion channel to notify.
    pending_publishes: Arc<Mutex<HashMap<u16, PendingPublish>>>,
}

struct PublishState {
    /// Receiver for messages that need to be sent.
    publish_queue_rx: mpsc::Receiver<QueuedMessage>,

    /// Receiver for packet IDs of messages that have been sent.
    sent_queue_rx: mpsc::Receiver<u16>,

    pending_publishes: Arc<Mutex<HashMap<u16, PendingPublish>>>,
}

/// MqttierClient provides an abstracted, cloneable interface around `rumqttc`.
///
/// Usage contract:
/// - Construct with `MqttierClient::new(...)`.
/// - Call `run_loop().await` once per client to start background tasks.
/// - Use `subscribe(...)` to register a subscription and a channel sender for messages.
/// - Use `publish*` helpers to publish data; they return a oneshot receiver that will be
///   resolved when the publish completes (or timed out / error).
#[derive(Clone)]
pub struct MqttierClient {
    pub client_id: String,
    client: AsyncClient,
    state: Arc<RwLock<ClientState>>,
    next_subscription_id: Arc<AtomicUsize>,
    is_running: Arc<Mutex<bool>>,
    eventloop: Arc<Mutex<Option<EventLoop>>>,

    // All publishes from the client must be run through this queue.
    publish_queue_tx: mpsc::Sender<QueuedMessage>,

    /// Sender to indicate messages by packet ID which have been sent.
    sent_queue_tx: mpsc::Sender<u16>,

    /// Shared publish-related state (pending publishes, sent queue receiver, etc.)
    publish_state: Arc<Mutex<PublishState>>,
}

impl MqttierClient {
    /// Create a new `MqttierClient`.
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
        
        // Create the sent queue channels (u16 packet ids)
        let (sent_queue_tx, sent_queue_rx) = mpsc::channel::<u16>(5);
        let (publish_queue_tx, publish_queue_rx) = mpsc::channel::<QueuedMessage>(100);

        let initial_publish_state = PublishState {
            publish_queue_rx,
            sent_queue_rx,
            pending_publishes: Arc::new(Mutex::new(HashMap::new())),
        };

        let initial_state = ClientState {
            is_connected: false,
            subscriptions: HashMap::new(),
            queued_subscriptions: Vec::new(),
            pending_publishes: initial_publish_state.pending_publishes.clone(),
        };

        Ok(Self {
            client_id,
            client,
            state: Arc::new(RwLock::new(initial_state)),
            next_subscription_id: Arc::new(AtomicUsize::new(5)),
            is_running: Arc::new(Mutex::new(false)),
            eventloop: Arc::new(Mutex::new(Some(eventloop))),
            publish_queue_tx,
            sent_queue_tx,
            publish_state: Arc::new(Mutex::new(initial_publish_state)),
        })
    }

    /// Get the next subscription id value (internal).
    fn next_subscription_id(&self) -> usize {
        self.next_subscription_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Subscribe to a topic.
    /// 
    /// # Arguments
    /// 
    ///
    /// Arguments:
    /// - `topic`: topic to subscribe to
    /// - `qos`: QoS level (0, 1, 2)
    /// - `received_message_tx`: mpsc Sender that will receive `ReceivedMessage`s for this subscription
    ///
    /// Returns: subscription id (usize) on success.
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

    /// Publish a raw payload to a topic.
    /// 
    /// # Arguments
    /// 
    /// * `topic` - The topic to publish to
    ///
    /// Arguments:
    /// - `topic`: topic to publish to
    /// - `payload`: raw payload bytes
    /// - `qos`: QoS (use `rumqttc::v5::mqttbytes::QoS` variants)
    /// - `retain`: whether to set the retain flag
    /// - `publish_props`: optional MQTT5 publish properties
    ///
    /// Returns: a `oneshot::Receiver<PublishResult>` that will resolve when the publish
    /// completes or errors.
    pub async fn publish(&self, topic: String, payload: Vec<u8>, qos: QoS , retain: bool, publish_props: Option<PublishProperties>) -> oneshot::Receiver<PublishResult> {
        let _state = self.state.read().await; // keep to ensure we hold read access when checking connection if needed
        let publish_props = publish_props.unwrap_or_default();
        let (completion_tx, completion_rx) = oneshot::channel::<PublishResult>();

        // If we have a publish queue, send through that.
        debug!("Sending message to publish queue for topic: {} with QoS: {:?}", topic, qos);
        let message = QueuedMessage {
            topic,
            payload,
            qos,
            retain,
            publish_props,
            completion: Some(completion_tx),
        };
        match self.publish_queue_tx.send(message).await {
            Ok(_) => {}
            Err(e) => {
                // On error we get the message back so we can notify the original completion sender
                let mut returned = e.0;
                if let Some(sender) = returned.completion.take() {
                    let _ = sender.send(PublishResult::Error("Channel send error".to_string()));
                }
            }
        }

        completion_rx
    }

    /// Publish a UTF-8 string payload to a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The string payload to send
    /// * `qos` - The QoS level for the message (0, 1, or 2)
    /// * `retain` - Whether to retain the message
    /// * `publish_props` - Optional publish properties
    /// 
    /// # Returns
    /// 
    /// Returns a receiver that will be notified when the message is acknowledged by the broker
    pub async fn publish_string(
        &self,
        topic: String,
        payload: String,
        qos: u8,
        retain: bool,
        publish_props: Option<PublishProperties>,
    ) -> oneshot::Receiver<PublishResult> {
        let rumqttc_qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => {
                let (completion_tx, completion_rx) = oneshot::channel::<PublishResult>();
                completion_tx.send(PublishResult::Error(format!("Invalid QoS value: {}", qos))).unwrap(); // Unwrap, because if we can't send to this oneshot we just created, we're in big trouble.
                return completion_rx;
            }
        };
        self.publish(topic, payload.into_bytes(), rumqttc_qos, retain, publish_props).await
    }

    /// Publish a serializable struct as JSON to a topic (QoS 2 by default in helper).
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The serializable struct to send as payload
    /// * `state_version` - The version of the structure
    /// 
    /// # Returns
    /// 
    /// Returns a receiver that will be notified when the message is acknowledged by the broker
    pub async fn publish_structure<T: Serialize>(
        &self,
        topic: String,
        payload: T,
    ) -> oneshot::Receiver<PublishResult> {
        let mut props = PublishProperties::default();
        props.content_type = Some("application/json".to_string());
        match serde_json::to_vec(&payload){
            Ok(payload_bytes) => {
                self.publish(topic, payload_bytes, QoS::ExactlyOnce, false, Some(props)).await
            },
            Err(e) => {
                let (completion_tx, completion_rx) = oneshot::channel::<PublishResult>();
                completion_tx.send(PublishResult::SerializationError(format!("Serialization error: {}", e))).unwrap(); // Unwrap, because if we can't send to this oneshot we just created, we're in big trouble.
                return completion_rx
            }   
        }
    }

    /// Publish a request message to a topic with response topic and correlation id.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The serializable struct to send as payload
    /// * `response_topic` - The topic where responses should be sent
    /// * `correlation_id` - The correlation id for matching responses
    /// 
    /// # Returns
    /// 
    /// Returns a receiver that will be notified when the message is acknowledged by the broker
    pub async fn publish_request<T: Serialize>(
        &self,
        topic: String,
        payload: T,
        response_topic: String,
        correlation_id: Vec<u8>,
    ) -> oneshot::Receiver<PublishResult> {
        let mut props = PublishProperties::default();
        props.response_topic = Some(response_topic);
        props.correlation_data = Some(correlation_id.into());
        props.content_type = Some("application/json".to_string());
        match serde_json::to_vec(&payload){
            Ok(payload_bytes) => {
                self.publish(topic, payload_bytes, QoS::ExactlyOnce, false, Some(props)).await
            },
            Err(e) => {
                let (completion_tx, completion_rx) = oneshot::channel::<PublishResult>();
                completion_tx.send(PublishResult::SerializationError(format!("Serialization error: {}", e))).unwrap(); // Unwrap, because if we can't send to this oneshot we just created, we're in big trouble.
                return completion_rx;
            }
        }
    }

    /// Publish a response message to a topic with correlation id.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The serializable struct to send as payload
    /// * `correlation_id` - The correlation id for matching requests
    /// 
    /// # Returns
    /// 
    /// Returns a receiver that will be notified when the message is acknowledged by the broker
    pub async fn publish_response<T: Serialize>(
        &self,
        topic: String,
        payload: T,
        correlation_id: Vec<u8>,
    ) -> oneshot::Receiver<PublishResult> {
        let mut props = PublishProperties::default();
        props.content_type = Some("application/json".to_string());
        props.correlation_data = Some(correlation_id.into());
        match serde_json::to_vec(&payload){
            Ok(payload_bytes) => {
                self.publish(topic, payload_bytes, QoS::ExactlyOnce, false, Some(props)).await
            },
            Err(e) => {
                let (completion_tx, completion_rx) = oneshot::channel::<PublishResult>();
                completion_tx.send(PublishResult::SerializationError(format!("Serialization error: {}", e))).unwrap(); // Unwrap, because if we can't send to this oneshot we just created, we're in big trouble.
                return completion_rx;
            }
        }
    }

    /// Publish a state message to a topic. Adds a user property `PropertyVersion` to the message properties.
    ///
    /// # Arguments
    /// 
    /// * `topic` - The topic to publish to
    /// * `payload` - The serializable struct to send as payload
    /// 
    /// # Returns
    /// 
    /// Returns a receiver that will be notified when the message is acknowledged by the broker
    pub async fn publish_state<T: Serialize>(
        &self,
        topic: String,
        payload: T,
        state_version: u32,
    ) -> oneshot::Receiver<PublishResult> {
        let mut props = PublishProperties::default();
        props.user_properties.push((
            "PropertyVersion".to_string(),
            state_version.to_string(),
        ));
        props.content_type = Some("application/json".to_string());
        match serde_json::to_vec(&payload){
            Ok(payload_bytes) => {
                debug!("Publishing state to topic: {} with version: {}", topic, state_version);
                self.publish(topic, payload_bytes, QoS::AtLeastOnce, true, Some(props)).await
            },
            Err(e) => {
                let (completion_tx, completion_rx) = oneshot::channel::<PublishResult>();
                completion_tx.send(PublishResult::SerializationError(format!("Serialization error: {}", e))).unwrap(); // Unwrap, because if we can't send to this oneshot we just created, we're in big trouble.
                return completion_rx;
            }
        }
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
        let publish_state = self.publish_state.clone();
        let sent_queue_tx = self.sent_queue_tx.clone();

        // Start the publish loop
        let client_for_publish = client.clone();
        let state_for_publish = state.clone();
        tokio::spawn(async move {
            Self::publish_loop(client_for_publish, state_for_publish, publish_state).await;
        });

        // Start the connection loop
        tokio::spawn(async move {
            loop {
                info!("Starting MQTT connection loop");
                
                // Take ownership of the eventloop
                let mut eventloop_guard = eventloop.lock().await;
                if let Some(mut el) = eventloop_guard.take() {
                    drop(eventloop_guard);
                    
                    match Self::handle_connection(&client, &mut el, &state, sent_queue_tx.clone()).await {
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

    /// Handle the publish loop - processes queued messages and waits for acknowledgments
    async fn publish_loop(
        client: AsyncClient,
        state: Arc<RwLock<ClientState>>,
        publish_state: Arc<Mutex<PublishState>>,
    ) {
        info!("Starting publish loop");
        // This should be the only publish loop, so we can lock the publish state here.
        let mut pub_state = publish_state.lock().await;

    while let Some(mut message) = pub_state.publish_queue_rx.recv().await {
            debug!("Processing queued message for topic: {}", message.topic);
            let publish_result: std::result::Result<(), rumqttc::v5::ClientError> = client.publish_with_properties(
                &message.topic,
                message.qos,
                message.retain,
                message.payload,
                message.publish_props,
            ).await;

            match publish_result {
                Ok(_) => {
                    debug!("Published queued message for topic: {}", message.topic);

                    // After a successful publish call, wait for the sent packet id from the sent queue receiver
                    match tokio::time::timeout(Duration::from_secs(5), pub_state.sent_queue_rx.recv()).await {
                        Ok(Some(packet_id)) => {
                            debug!("Received sent packet id {} for topic: {}", packet_id, message.topic);
                            // Ensure the client is connected before registering the pending publish so we can correlate acks.
                            loop {
                                let state_read = state.read().await;
                                if state_read.is_connected {
                                    break;
                                }
                                drop(state_read);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }

                            // If QoS is 0, immediately notify completion and continue without inserting into pending_publishes
                            if message.qos == QoS::AtMostOnce {
                                if let Some(sender) = message.completion.take() {
                                    let _ = sender.send(PublishResult::Sent(packet_id));
                                }
                                continue;
                            }

                            if let Some(sender) = message.completion.take() {
                                let mut pp_map_guard = pub_state.pending_publishes.lock().await;
                                pp_map_guard.insert(packet_id, PendingPublish {
                                    qos: message.qos,
                                    completion: sender,
                                });
                            } else {
                                error!("No completion channel provided for published message on topic: {}", message.topic);
                            }
                        }
                        Ok(None) => {
                            // sent_queue receiver closed unexpectedly
                            error!("sent_queue_rx closed unexpectedly; cannot correlate published message for topic: {}", message.topic);
                            if let Some(sender) = message.completion.take() {
                                let _ = sender.send(PublishResult::Error("sent_queue receiver closed".to_string()));
                            }
                        }
                        Err(_) => {
                            // Timeout waiting for sent packet id
                            warn!("Timed out waiting for sent packet id for topic: {}", message.topic);
                            if let Some(sender) = message.completion.take() {
                                let _ = sender.send(PublishResult::TimedOut);
                            }
                            continue;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to publish message to {}: {}", message.topic, e);
                    if let Some(sender) = message.completion.take() {
                        let _ = sender.send(PublishResult::Error(format!("{}", e)));
                    }
                }
            }
        }
    }

    /// Handle a single MQTT connection
    async fn handle_connection(
        client: &AsyncClient,
        eventloop: &mut EventLoop,
        state: &Arc<RwLock<ClientState>>,
        sent_queue_tx: mpsc::Sender<u16>,
    ) -> Result<()> {

        loop {
            debug!("Event loop polled");
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("CONNACK: Connected to MQTT broker");

                    {
                        let mut state_guard = state.write().await;
                        state_guard.is_connected = true;
                    }

                    // Process queued subscriptions
                    let s = state.clone();
                    let c = client.clone();
                    tokio::spawn(async move {
                        let mut state_guard = s.write().await;
                        for subscription in state_guard.queued_subscriptions.drain(..) {
                            debug!("Processing queued subscription for topic: {}", subscription.topic);
                            if let Err(e) = c.subscribe_with_properties(&subscription.topic, subscription.qos, subscription.props).await {
                                error!("Failed to subscribe to {}: {}", subscription.topic, e);
                            }
                        }
                    });

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
                            // Collect user properties into a HashMap (last value wins for duplicate keys)
                            let mut user_props_map: HashMap<String, String> = HashMap::new();
                            for (k, v) in pub_props.user_properties.iter() {
                                user_props_map.insert(k.clone(), v.clone());
                            }

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
                                user_properties: user_props_map,
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
                Ok(Event::Incoming(Packet::PubAck(puback))) => {
                    debug!("Received PUBACK for packet ID: {}", puback.pkid);
                    let pkid_u16 = puback.pkid;

                    // If we have a pending publish for this packet id and it was QoS 1, notify completion as Acknowledged.
                    {
                        let pending_arc = state.read().await.pending_publishes.clone();
                        let mut pending_map = pending_arc.lock().await;
                        if let Some(existing) = pending_map.get(&pkid_u16) {
                            if existing.qos == QoS::AtLeastOnce {
                                if let Some(pending) = pending_map.remove(&pkid_u16) {
                                    let _ = pending.completion.send(PublishResult::Acknowledged(pkid_u16));
                                }
                            }
                        }
                    }
                }
                Ok(Event::Incoming(Packet::PubComp(pubcomp))) => {
                    debug!("Received PUBCOMP for packet ID: {}", pubcomp.pkid);
                    let pkid_u16 = pubcomp.pkid;
                    // If we have a pending publish for this packet id and it was QoS 12, notify completion as Completed.
                    {
                        let pending_arc = state.read().await.pending_publishes.clone();
                        let mut pending_map = pending_arc.lock().await;
                        if let Some(existing) = pending_map.get(&pkid_u16) {
                            if existing.qos == QoS::ExactlyOnce {
                                if let Some(pending) = pending_map.remove(&pkid_u16) {
                                    let _ = pending.completion.send(PublishResult::Completed(pkid_u16));
                                }
                            }
                        }
                    }
                }
                Ok(Event::Incoming(packet)) => {
                    debug!("Received packet: {:?}", packet);
                }
                Ok(Event::Outgoing(outgoing)) => {
                    debug!("Outgoing event: {:?}", outgoing);
                    // If the outgoing packet is a Publish, extract its packet id and send it to sent_queue_tx
                    if let rumqttc::Outgoing::Publish(pkid) = outgoing {
                        if let Err(e) = sent_queue_tx.send(pkid).await {
                            error!("Failed to send pkid {} to sent_queue_tx: {}", pkid, e);
                        }
                    }
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

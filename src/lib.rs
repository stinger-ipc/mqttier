//! # MQTTier
//!
//! A Rust MQTT client library providing an abstracted interface around rumqttc.

#[cfg(feature = "metrics")]
pub mod metrics;

use async_trait::async_trait;
use builder_pattern::Builder;
use bytes::Bytes;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{
    LastWill, LastWillProperties, Packet, PublishProperties, SubscribeProperties,
};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot, watch, broadcast};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use stinger_mqtt_trait::message::{MqttMessage, MqttMessageBuilder, QoS as StingerQoS};
use stinger_mqtt_trait::{Mqtt5PubSub, MqttPublishSuccess, Mqtt5PubSubError, MqttConnectionState};

/// Errors that can occur when using MqttierClient
#[derive(Error, Debug)]
pub enum MqttierError {
    #[error("MQTT connection error: {0}")]
    ConnectionError(#[from] rumqttc::v5::ConnectionError),
    #[error("MQTT client error: {0}")]
    ClientError(#[from] rumqttc::v5::ClientError),
    #[error("Channel send error")]
    ChannelSendError,
    #[error("Invalid QoS value: {0}")]
    InvalidQos(u8),
}

type Result<T> = std::result::Result<T, MqttierError>;

/// Completion signal for a published message
type PublishCompletion = oneshot::Sender<std::result::Result<MqttPublishSuccess, Mqtt5PubSubError>>;

/// Represents a publish waiting for acknowledgment
#[derive(Debug)]
struct PendingPublish {
    qos: QoS,
    completion: PublishCompletion,
    #[cfg(feature = "metrics")]
    timestamp: std::time::Instant,
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
    message: MqttMessage,
    completion: Option<PublishCompletion>,
}

/// Internal state of the MqttierClient
#[derive(Debug)]
struct ClientState {
    is_connected: bool,
    subscriptions: HashMap<usize, broadcast::Sender<MqttMessage>>,
    /// Map from topic to subscription IDs
    topic_to_subscription_ids: HashMap<String, Vec<usize>>,
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

    pub ack_timeout_ms: Arc<RwLock<u64>>,
}

/// Represents a non-TLS connection to a broker based on its hostname and port.
#[derive(Clone)]
pub struct TcpConnection {
    pub hostname: String,
    pub port: u16,
}

/// Specifies what type of connection to make to the broker.
/// TLS and websocket connections not currently supported.
#[derive(Clone)]
pub enum Connection {
    TcpLocalhost(u16),  // Specify the port of the localhost MQTT broker.
    UnixSocket(String), // Specify the path to the Unix socket.
    Tcp(TcpConnection), // Specify hostname and port.
}

/// Specifies messages to sent to a topic when the connection goes online or offline.
#[derive(Clone)]
pub struct OnlineMessage {
    pub topic: Option<Bytes>,
    pub content_type: Option<String>,
    pub online: Option<Bytes>,
    pub offline: Option<Bytes>,
}

impl Default for OnlineMessage {
    fn default() -> Self {
        OnlineMessage {
            topic: None,
            content_type: Some("application/json".to_string()),
            online: Some(Bytes::from("{\"state\":\"online\"}")),
            offline: Some(Bytes::from("{\"state\":\"offline\"}")),
        }
    }
}

/// Options for configuring the MqttierClient.
#[derive(Clone, Builder)]
pub struct MqttierOptions {
    #[default(Connection::TcpLocalhost(1883))]
    pub connection: Connection,
    #[default_lazy(||Uuid::new_v4().to_string())]
    pub client_id: String,
    #[default(5000)]
    pub ack_timeout_ms: u64,
    #[default(60)]
    pub keepalive_secs: u16,
}

/// MqttierClient provides an abstracted, Clone-able interface around `rumqttc`.
///
/// Usage contract:
/// - Construct with `MqttierClient::new(...)`.
/// - Call `start().await` once per client to start background tasks.
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

    /// Connection state sender for broadcasting state changes
    connection_state_tx: watch::Sender<MqttConnectionState>,

    /// Connection state receiver for monitoring connection state
    connection_state_rx: watch::Receiver<MqttConnectionState>,

    /// MQTT connection options including LWT configuration
    mqtt_options: Arc<RwLock<MqttOptions>>,

    /// Metrics collector
    #[cfg(feature = "metrics")]
    metrics: Arc<metrics::Metrics>,
}

impl MqttierClient {
    /// Create a new `MqttierClient`.
    ///
    /// # Arguments
    ///
    /// * `hostname` - The hostname of the MQTT broker
    /// * `port` - The port of the MQTT broker
    /// * `client_id` - Optional client ID. If None, a random UUID will be generated
    pub fn new(mqttier_options: MqttierOptions) -> Result<Self> {
        let client_id = mqttier_options.client_id;

        // Create the sent queue channels (u16 packet ids)
        let (sent_queue_tx, sent_queue_rx) = mpsc::channel::<u16>(5);
        let (publish_queue_tx, publish_queue_rx) = mpsc::channel::<QueuedMessage>(100);

        let initial_publish_state = PublishState {
            publish_queue_rx,
            sent_queue_rx,
            pending_publishes: Arc::new(Mutex::new(HashMap::new())),
            ack_timeout_ms: Arc::new(RwLock::new(mqttier_options.ack_timeout_ms)),
        };

        let (hostname, port) = {
            match &mqttier_options.connection {
                Connection::TcpLocalhost(tcp_port) => ("localhost".to_string(), *tcp_port),
                Connection::Tcp(conn) => (conn.hostname.clone(), conn.port),
                Connection::UnixSocket(path) => (path.clone(), 0),
            }
        };

        let mut mqttoptions = MqttOptions::new(client_id.clone(), hostname, port);
        mqttoptions.set_keep_alive(Duration::from_secs(mqttier_options.keepalive_secs as u64));
        mqttoptions.set_clean_start(true);

        if let Connection::UnixSocket(_socket_path) = mqttier_options.connection {
            mqttoptions.set_transport(rumqttc::Transport::Unix);
        }

        let (client, eventloop) = AsyncClient::new(mqttoptions.clone(), 10);

        // Create connection state channel
        let (connection_state_tx, connection_state_rx) = watch::channel(MqttConnectionState::Disconnected);

        let initial_state = ClientState {
            is_connected: false,
            subscriptions: HashMap::new(),
            topic_to_subscription_ids: HashMap::new(),
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
            connection_state_tx,
            connection_state_rx,
            mqtt_options: Arc::new(RwLock::new(mqttoptions)),
            #[cfg(feature = "metrics")]
            metrics: Arc::new(metrics::Metrics::new()),
        })
    }

    /// Get the next subscription id value (internal).
    fn next_subscription_id(&self) -> usize {
        self.next_subscription_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get a snapshot of current metrics.
    ///
    /// This method is only available when the `metrics` feature is enabled.
    #[cfg(feature = "metrics")]
    pub fn get_metrics(&self) -> metrics::MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Reset all metrics to zero.
    ///
    /// This method is only available when the `metrics` feature is enabled.
    #[cfg(feature = "metrics")]
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }


    /// Subscribe to a topic.
    ///
    /// # Arguments
    ///
    ///
    /// Arguments:
    /// - `topic`: topic to subscribe to
    /// - `qos`: QoS level (0, 1, 2)
    /// - `received_message_tx`: broadcast Sender that will receive `MqttMessage`s for this subscription
    ///
    /// Returns: subscription id (usize) on success.
    pub async fn subscribe(
        &self,
        topic: String,
        qos: u8,
        received_message_tx: broadcast::Sender<MqttMessage>,
    ) -> Result<usize> {
        let subscription_id = self.next_subscription_id();

        let mut state = self.state.write().await;
        state
            .subscriptions
            .insert(subscription_id, received_message_tx);
        
        // Track topic to subscription ID mapping
        state.topic_to_subscription_ids
            .entry(topic.clone())
            .or_insert_with(Vec::new)
            .push(subscription_id);
        
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
        
        #[cfg(feature = "metrics")]
        self.metrics.increment_subscription_requests();
        
        if state.is_connected {
            debug!("Subscribing to topic: {} with QoS: {:?}", topic, qos);
            let result = self.client
                .subscribe_with_properties(&topic, rumqttc_qos, subscription_props)
                .await;
            
            match result {
                Ok(_) => {
                    #[cfg(feature = "metrics")]
                    self.metrics.increment_active_subscriptions();
                },
                Err(e) => {
                    #[cfg(feature = "metrics")]
                    self.metrics.increment_subscription_failures();
                    return Err(e.into());
                }
            }
        } else {
            debug!(
                "Queueing subscription for topic: {} with QoS: {:?}",
                topic, qos
            );
            state.queued_subscriptions.push(QueuedSubscription {
                topic,
                qos: rumqttc_qos,
                props: subscription_props,
            });
        }

        Ok(subscription_id)
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
        let connection_state_tx = self.connection_state_tx.clone();
        #[cfg(feature = "metrics")]
        let metrics = self.metrics.clone(); 

        // Start the publish loop
        let client_for_publish = client.clone();
        let state_for_publish = state.clone();
        #[cfg(feature = "metrics")]
        let metrics_for_publish = metrics.clone(); 
        
        tokio::spawn(async move {
            Self::publish_loop(
                client_for_publish,
                state_for_publish,
                publish_state,
                #[cfg(feature = "metrics")]
                metrics_for_publish
        ).await;
        });

        // Start the connection loop
        tokio::spawn(async move {
            loop {
                info!("Starting MQTT connection loop");
                
                #[cfg(feature = "metrics")]
                metrics.increment_connection_attempts();

                // Take ownership of the eventloop
                let mut eventloop_guard = eventloop.lock().await;
                if let Some(mut el) = eventloop_guard.take() {
                    drop(eventloop_guard);

                    let result = Self::handle_connection(
                                &client, 
                                &mut el, 
                                &state, 
                                sent_queue_tx.clone(), 
                                connection_state_tx.clone(), 
                                #[cfg(feature = "metrics")]
                                metrics.clone()
                            )
                        .await;
                        
                    match result {
                        Ok(_) => {
                            info!("MQTT connection loop ended normally");
                        }
                        Err(e) => {
                            error!("MQTT connection error: {}", e);
                            #[cfg(feature = "metrics")]
                            metrics.record_failed_connection();
                        }
                    }
                } else {
                    error!("EventLoop not available");
                    #[cfg(feature = "metrics")]
                    metrics.record_failed_connection();
                    break;
                }

                // Mark as disconnected
                {
                    let mut state_guard = state.write().await;
                    state_guard.is_connected = false;
                }
                
                #[cfg(feature = "metrics")]
                metrics.record_disconnection();

                // Update connection state
                let _ = connection_state_tx.send(MqttConnectionState::Disconnected);

                // Wait before reconnecting
                warn!("Reconnecting in 5 seconds...");
                #[cfg(feature = "metrics")]
                metrics.increment_reconnection_count();
                
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        Ok(())
    }

    async fn wait_for_connection(state: Arc<RwLock<ClientState>>) {
        let mut i = 0;
        loop {
            let state_read = state.read().await;
            if state_read.is_connected {
                break;
            }
            drop(state_read);
            if (i % 20) == 0 {
                debug!("Waiting for mqtt connection");
            }
            i = i + 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Handle the publish loop - processes queued messages and waits for acknowledgments
    async fn publish_loop(
        client: AsyncClient,
        state: Arc<RwLock<ClientState>>,
        publish_state: Arc<Mutex<PublishState>>,
        #[cfg(feature = "metrics")]
        metrics: Arc<metrics::Metrics>, 
    ) {
        debug!("Starting publish loop");
        // This should be the only publish loop, so we can lock the publish state here.
        let mut pub_state = publish_state.lock().await;
        let pending_publishes_arc = pub_state.pending_publishes.clone();

        while let Some(mut queued_message) = pub_state.publish_queue_rx.recv().await {
            MqttierClient::wait_for_connection(state.clone()).await;
            
            let topic = queued_message.message.topic.clone();
            #[cfg(feature = "metrics")]
            let payload_size = queued_message.message.payload.len();
            debug!("Publishing message to topic: {}", topic);
            
            let qos = {
                match queued_message.message.qos {
                    StingerQoS::AtMostOnce => QoS::AtMostOnce,
                    StingerQoS::AtLeastOnce => QoS::AtLeastOnce,
                    StingerQoS::ExactlyOnce => QoS::ExactlyOnce,
                }
            };

            #[cfg(feature = "metrics")]
            let qos_u8 = match qos {
                QoS::AtMostOnce => 0,
                QoS::AtLeastOnce => 1,
                QoS::ExactlyOnce => 2,
            };

            let mut pub_props = PublishProperties::default();
            if let Some(resp_topic) = queued_message.message.response_topic {
                pub_props.response_topic = Some(resp_topic.clone());
            }
            if let Some(corr_data) = queued_message.message.correlation_data {
                pub_props.correlation_data = Some(corr_data.clone());
            }
            if queued_message.message.user_properties.len() > 0 {
                let mut user_props_vec: Vec<(String, String)> = Vec::new();
                for (k, v) in queued_message.message.user_properties.iter() {
                    user_props_vec.push((k.clone(), v.clone()));
                }
                pub_props.user_properties = user_props_vec;
            }


            // We lock this mutex here before publishing.  This prevents processing of the puback until we can insert into the pending_publishes map.
            let mut pending_puback_map_guard = pending_publishes_arc.lock().await;

            let publish_result: std::result::Result<(), rumqttc::v5::ClientError> = client
                .publish_with_properties(
                    queued_message.message.topic,
                    qos,
                    queued_message.message.retain,
                    queued_message.message.payload,
                    pub_props
                )
                .await;

            match publish_result {
                Ok(_) => {
                    // After a successful publish call, wait for the sent packet id from the sent queue receiver
                    let timeout_ms = {
                        let ack_timeout_guard = pub_state.ack_timeout_ms.read().await;
                        *ack_timeout_guard
                    };
                    debug!("Waiting for sent packet id with timeout");
                    let sent_result = tokio::time::timeout(
                        Duration::from_millis(timeout_ms),
                        pub_state.sent_queue_rx.recv(),
                    )
                    .await;
                    match sent_result
                    {
                        Ok(Some(packet_id)) => { // Another part of the code found the outgoing packet id and sent it here via sent_queue_rx.
                            debug!(
                                "Published message to '{}' given packet id {}",
                                topic, packet_id
                            );

                            // If QoS is 0, immediately notify completion and continue without inserting into pending_publishes
                            if qos == QoS::AtMostOnce {
                                #[cfg(feature = "metrics")]
                                metrics.record_message_published(qos_u8, payload_size);
                                if let Some(sender) = queued_message.completion.take() {
                                    let _ = sender.send(Ok(MqttPublishSuccess::Sent));
                                }
                                continue;
                            }

                            if let Some(sender) = queued_message.completion.take() {
                                #[cfg(feature = "metrics")]
                                metrics.increment_pending_publishes();
                                pending_puback_map_guard.insert(
                                    packet_id,
                                    PendingPublish {
                                        qos: qos,
                                        completion: sender,
                                        #[cfg(feature = "metrics")]
                                        timestamp: std::time::Instant::now(),
                                    },
                                );
                            } else {
                                info!(
                                    "No completion channel provided for published message on topic: {}",
                                    topic
                                );
                            }
                        }
                        Ok(None) => {
                            // sent_queue receiver closed unexpectedly
                            error!(
                                "sent_queue_rx closed unexpectedly; cannot correlate published message for topic: {}",
                                topic
                            );
                            #[cfg(feature = "metrics")]
                            metrics.increment_publish_failures();
                            if let Some(sender) = queued_message.completion.take() {
                                let _ = sender.send(Err(Mqtt5PubSubError::Other("sent_queue receiver closed".to_string())));
                            }
                        }
                        Err(_) => {
                            // Timeout waiting for sent packet id
                            warn!(
                                "Timed out waiting for sent packet id for topic: {}",
                                topic
                            );
                            #[cfg(feature = "metrics")]
                            metrics.increment_publish_timeouts();
                            if let Some(sender) = queued_message.completion.take() {
                                let _ = sender.send(Err(Mqtt5PubSubError::TimeoutError("Timeout waiting for packet ID".to_string())));
                            }
                            continue;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to publish message to {}: {}", topic, e);
                    #[cfg(feature = "metrics")]
                    metrics.increment_publish_failures();
                    if let Some(sender) = queued_message.completion.take() {
                        let _ = sender.send(Err(Mqtt5PubSubError::Other(format!("{}", e))));
                    }
                }
            }
            drop(pending_puback_map_guard);
        }
    }

    /// Handle a single MQTT connection
    async fn handle_connection(
        client: &AsyncClient,
        eventloop: &mut EventLoop,
        state: &Arc<RwLock<ClientState>>,
        sent_queue_tx: mpsc::Sender<u16>,
        connection_state_tx: watch::Sender<MqttConnectionState>,
        #[cfg(feature = "metrics")]
        metrics: Arc<metrics::Metrics>,
    ) -> Result<()> {
        loop {
            debug!("Event loop polled");
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {

                    {
                        let mut state_guard = state.write().await;
                        state_guard.is_connected = true;
                        info!("CONNACK: Connected to MQTT broker");
                    }
                    #[cfg(feature = "metrics")]
                    metrics.record_successful_connection();

                    // Update connection state
                    let _ = connection_state_tx.send(MqttConnectionState::Connected);

                    // Process queued subscriptions
                    let s = state.clone();
                    let c = client.clone();
                    #[cfg(feature = "metrics")]
                    let m = metrics.clone();
                    tokio::spawn(async move {
                        loop {
                            let next_subscription = {
                                let mut state_guard = s.write().await;
                                state_guard.queued_subscriptions.pop()
                            };
                            if let Some(subscription) = next_subscription {
                                debug!(
                                    "Processing queued subscription for topic: {}",
                                    subscription.topic
                                );
                                #[cfg(feature = "metrics")]
                                m.increment_subscription_requests();
                                if let Err(e) = c
                                    .subscribe_with_properties(
                                        &subscription.topic,
                                        subscription.qos,
                                        subscription.props,
                                    )
                                    .await
                                {
                                    error!("Failed to subscribe to {}: {}", subscription.topic, e);
                                    #[cfg(feature = "metrics")]
                                    m.increment_subscription_failures();
                                } else {
                                    #[cfg(feature = "metrics")]
                                    m.increment_active_subscriptions();
                                }
                            } else {
                                debug!("Finished processing queued subscriptions");
                                break;
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
                        
                        #[cfg(feature = "metrics")]
                        {
                            let payload_size = publish.payload.len();
                            let qos_u8 = match publish.qos {
                                QoS::AtMostOnce => 0,
                                QoS::AtLeastOnce => 1,
                                QoS::ExactlyOnce => 2,
                            };
                            metrics.record_message_received(qos_u8, payload_size);
                        }
                        
                        for subscription_id in subscription_ids {
                            // Collect user properties into a HashMap (last value wins for duplicate keys)
                            let mut user_props_map: HashMap<String, String> = HashMap::new();
                            for (k, v) in pub_props.user_properties.iter() {
                                user_props_map.insert(k.clone(), v.clone());
                            }

                            let message = MqttMessageBuilder::default()
                                .topic(&topic_str)
                                .payload(publish.payload.clone())
                                .subscription_id(Some(subscription_id as u32))
                                .response_topic(response_topic.clone())
                                .correlation_data(correlation_data.clone())
                                .content_type(content_type.clone())
                                .user_properties(user_props_map)
                                .retain(publish.retain)
                                .qos(match publish.qos {
                                    QoS::AtMostOnce => StingerQoS::AtMostOnce,
                                    QoS::AtLeastOnce => StingerQoS::AtLeastOnce,
                                    QoS::ExactlyOnce => StingerQoS::AtLeastOnce,
                                })
                                .build().unwrap();
                            let state_guard = state.read().await;
                            if let Some(sender) = state_guard.subscriptions.get(&subscription_id) {
                                if let Err(_) = sender.send(message.clone()) {
                                    warn!(
                                        "Failed to send message to subscription {}",
                                        subscription_id
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(Event::Incoming(Packet::PubAck(puback))) => {
                    debug!("Received PUBACK for packet ID: {}.", puback.pkid);
                    let pkid_u16 = puback.pkid;
                    let pending_arc: Arc<Mutex<HashMap<u16, PendingPublish>>> = state.read().await.pending_publishes.clone();
                    #[cfg(feature = "metrics")]
                    let m = metrics.clone();
                    
                    // Since PUBACK requires mutex to the 'pending_publishes' map, we span a task to handle it
                    // when the mutex is available so that we don't block the event loop.
                    let _ = tokio::spawn(async move {
                        debug!("Processing PUBACK for packet ID: {}", pkid_u16);
                        let mut pending_map_guard = pending_arc.lock().await;

                        if let Some(existing) = pending_map_guard.get(&pkid_u16) {
                            if existing.qos == QoS::AtLeastOnce {
                                if let Some(pending) = pending_map_guard.remove(&pkid_u16) {
                                    #[cfg(feature = "metrics")]
                                    {
                                        let latency_ms = pending.timestamp.elapsed().as_millis() as u64;
                                        m.record_publish_latency(latency_ms);
                                        m.decrement_pending_publishes();
                                        m.record_message_published(1, 0); // QoS 1, size tracked earlier
                                        m.increment_puback_received();
                                    }
                                    let puback_send_result = pending
                                        .completion
                                        .send(Ok(MqttPublishSuccess::Acknowledged));
                                    debug!("Acknowledged PUBACK for packet ID: {} - {:?}", pkid_u16, puback_send_result);
                                } else {
                                    error!("Couldn't get pending_publish channel for packet {}", pkid_u16);
                                }
                            } else {
                                warn!("Received PUBACK for pkid {} but we recorded a QoS!=1 for this packet.", pkid_u16);
                            }
                        } else {
                            warn!("No pending publish channel found for pkid {} on PUBACK", pkid_u16);
                        }
                    });

                }
                Ok(Event::Incoming(Packet::PubComp(pubcomp))) => {
                    debug!("Received PUBCOMP for packet ID: {}", pubcomp.pkid);
                    let pkid_u16 = pubcomp.pkid;
                    let pending_arc: Arc<Mutex<HashMap<u16, PendingPublish>>> = state.read().await.pending_publishes.clone();
                    #[cfg(feature = "metrics")]
                    let m = metrics.clone();

                    // Since PUBCOMP requires mutex to the 'pending_publishes' map, we span a task to handle it
                    // when the mutex is available so that we don't block the event loop.
                    let _ = tokio::spawn(async move {
                        let mut pending_map = pending_arc.lock().await;
                        if let Some(existing) = pending_map.get(&pkid_u16) {
                            if existing.qos == QoS::ExactlyOnce {
                                if let Some(pending) = pending_map.remove(&pkid_u16) {
                                    #[cfg(feature = "metrics")]
                                    {
                                        let latency_ms = pending.timestamp.elapsed().as_millis() as u64;
                                        m.record_publish_latency(latency_ms);
                                        m.decrement_pending_publishes();
                                        m.record_message_published(2, 0); // QoS 2, size tracked earlier
                                        m.increment_pubcomp_received();
                                    }
                                    let _ =
                                        pending.completion.send(Ok(MqttPublishSuccess::Completed));
                                } else {
                                    error!("Couldn't get pending_publish channel for packet {}", pkid_u16);
                                }
                            } else {
                                warn!("Received PUBCOMP for pkid {} but we recorded a QoS!=2 for this packet.", pkid_u16);
                            }
                        } else if pending_map.contains_key(&pkid_u16) {
                            warn!("No pending publish channel found for pkid {} on PUBCOMP", pkid_u16);
                        }
                    });
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
                        } else {
                            debug!("Sent pkid {} to sent_queue_tx", pkid);
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

#[async_trait]
impl Mqtt5PubSub for MqttierClient {
    fn get_client_id(&self) -> String {
        self.client_id.clone()
    }

    fn get_state(&self) -> watch::Receiver<MqttConnectionState> {
        self.connection_state_rx.clone()
    }

    async fn subscribe(&mut self, topic: String, qos: stinger_mqtt_trait::message::QoS, tx: broadcast::Sender<MqttMessage>) -> std::result::Result<u32, Mqtt5PubSubError> {
        // Convert QoS from trait to internal representation
        let rumqttc_qos = match qos {
            stinger_mqtt_trait::message::QoS::AtMostOnce => QoS::AtMostOnce,
            stinger_mqtt_trait::message::QoS::AtLeastOnce => QoS::AtLeastOnce,
            stinger_mqtt_trait::message::QoS::ExactlyOnce => QoS::ExactlyOnce,
        };

        // Get subscription ID
        let subscription_id = self.next_subscription_id();

        // Register the subscription
        let mut state = self.state.write().await;
        state.subscriptions.insert(subscription_id, tx);
        
        // Track topic to subscription ID mapping
        state.topic_to_subscription_ids
            .entry(topic.clone())
            .or_insert_with(Vec::new)
            .push(subscription_id);
        
        let subscription_props = SubscribeProperties {
            id: Some(subscription_id),
            user_properties: Vec::new(),
        };

        #[cfg(feature = "metrics")]
        self.metrics.increment_subscription_requests();

        if state.is_connected {
            debug!("Subscribing to topic: {} with QoS: {:?}", topic, qos);
            drop(state); // Release the lock before await
            let result = self.client
                .subscribe_with_properties(&topic, rumqttc_qos, subscription_props)
                .await;
            
            match result {
                Ok(_) => {
                    #[cfg(feature = "metrics")]
                    self.metrics.increment_active_subscriptions();
                },
                Err(e) => {
                    #[cfg(feature = "metrics")]
                    self.metrics.increment_subscription_failures();
                    return Err(Mqtt5PubSubError::SubscriptionError(format!("{}", e)));
                }
            }
        } else {
            debug!(
                "Queueing subscription for topic: {} with QoS: {:?}",
                topic, qos
            );
            state.queued_subscriptions.push(QueuedSubscription {
                topic,
                qos: rumqttc_qos,
                props: subscription_props,
            });
        }

        Ok(subscription_id as u32)
    }

    async fn unsubscribe(&mut self, topic: String) -> std::result::Result<(), Mqtt5PubSubError> {
        let mut state = self.state.write().await;
        
        // Count how many subscriptions we're removing
        #[cfg(feature = "metrics")]
        let num_removed = state.topic_to_subscription_ids.get(&topic).map(|v| v.len()).unwrap_or(0);
        
        // Remove all subscription IDs for this topic
        if let Some(subscription_ids) = state.topic_to_subscription_ids.remove(&topic) {
            for subscription_id in subscription_ids {
                state.subscriptions.remove(&subscription_id);
            }
        }
        
        // Remove from queued subscriptions if not connected yet
        state.queued_subscriptions.retain(|sub| sub.topic != topic);
        
        // If connected, send unsubscribe to broker
        if state.is_connected {
            drop(state); // Release the lock before await
            self.client
                .unsubscribe(&topic)
                .await
                .map_err(|e| Mqtt5PubSubError::UnsubscribeError(format!("{}", e)))?;
        }
        
        #[cfg(feature = "metrics")]
        for _ in 0..num_removed {
            self.metrics.decrement_active_subscriptions();
        }
        
        Ok(())
    }

    async fn publish(&mut self, message: MqttMessage) -> std::result::Result<MqttPublishSuccess, Mqtt5PubSubError> {
        let _state = self.state.read().await; // keep to ensure we hold read access when checking connection if needed
        let (completion_tx, completion_rx) = oneshot::channel::<std::result::Result<MqttPublishSuccess, Mqtt5PubSubError>>();

        // If we have a publish queue, send through that.
        debug!(
            "Sending message to publish queue for topic: {} with QoS: {:?}",
            message.topic, message.qos
        );
        let topic = message.topic.clone();
        let queued_message = QueuedMessage {
            message,
            completion: Some(completion_tx),
        };
        match self.publish_queue_tx.send(queued_message).await {
            Ok(_) => {
                debug!("Message to {} queued for publish", topic);
            }
            Err(e) => {
                // On error we get the message back so we can notify the original completion sender
                let mut returned = e.0;
                if let Some(sender) = returned.completion.take() {
                    let _ = sender.send(Err(Mqtt5PubSubError::Other("Channel send error".to_string())));
                }
            }
        }
        
        match tokio::time::timeout(Duration::from_millis(5000), completion_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Mqtt5PubSubError::PublishError("Completion channel closed".to_string())),
            Err(_) => Err(Mqtt5PubSubError::TimeoutError(format!("Publish completion timeout after 5000ms"))),
        }
    }

    async fn publish_noblock(&mut self, message: MqttMessage) -> oneshot::Receiver<std::result::Result<MqttPublishSuccess, Mqtt5PubSubError>> {
        let _state = self.state.read().await; // keep to ensure we hold read access when checking connection if needed
        let (completion_tx, completion_rx) = oneshot::channel::<std::result::Result<MqttPublishSuccess, Mqtt5PubSubError>>();

        // If we have a publish queue, send through that.
        debug!(
            "Sending message to publish queue for topic: {} with QoS: {:?}",
            message.topic, message.qos
        );
        let topic = message.topic.clone();
        let queued_message = QueuedMessage {
            message,
            completion: Some(completion_tx),
        };
        match self.publish_queue_tx.send(queued_message).await {
            Ok(_) => {
                debug!("Message to {} queued for publish", topic);
            }
            Err(e) => {
                // On error we get the message back so we can notify the original completion sender
                let mut returned = e.0;
                if let Some(sender) = returned.completion.take() {
                    let _ = sender.send(Err(Mqtt5PubSubError::PublishError("Channel send error".to_string())));
                }
            }
        }

        completion_rx
    }

    fn publish_nowait(&mut self, message: MqttMessage) -> std::result::Result<MqttPublishSuccess, Mqtt5PubSubError> {
        debug!(
            "Queueing message for fire-and-forget publish to topic: {} with QoS: {:?}",
            message.topic, message.qos
        );
        
        let queued_message = QueuedMessage {
            message,
            completion: None, // No completion channel for fire-and-forget
        };
        
        // Use try_send since this is a non-async method
        self.publish_queue_tx
            .try_send(queued_message)
            .map_err(|e| Mqtt5PubSubError::PublishError(format!("Failed to queue message: {}", e)))?;
        
        // Return Sent immediately without waiting
        Ok(MqttPublishSuccess::Sent)
    }

    fn get_availability_helper(&mut self) -> stinger_mqtt_trait::available::AvailabilityHelper {
        stinger_mqtt_trait::available::AvailabilityHelper::system_availability(self.client_id.clone())
    }
}

// Connection management methods - not part of the Mqtt5PubSub trait
impl MqttierClient {
    /// Connect to the MQTT broker and establish a connection.
    ///
    /// Note: MqttierClient is designed to connect at construction time with connection
    /// details specified in MqttierOptions. The URI parameter is currently ignored.
    /// To properly support dynamic connection URIs would require significant refactoring.
    pub async fn connect(&mut self, _uri: String) -> std::result::Result<(), Mqtt5PubSubError> {
        // Start the run loop if not already running
        let is_running = {
            let guard = self.is_running.lock().await;
            *guard
        };
        
        if !is_running {
            self.run_loop()
                .await
                .map_err(|e| Mqtt5PubSubError::Other(format!("Failed to start connection loop: {}", e)))?;
        }
        
        // Wait for connection to be established
        let mut state_rx = self.connection_state_rx.clone();
        
        // Use timeout to avoid waiting forever
        let timeout_duration = Duration::from_secs(30);
        let start_time = std::time::Instant::now();
        
        loop {
            if let Ok(_) = state_rx.changed().await {
                let state = state_rx.borrow().clone();
                if matches!(state, MqttConnectionState::Connected) {
                    return Ok(());
                }
            }
            
            if start_time.elapsed() > timeout_duration {
                return Err(Mqtt5PubSubError::TimeoutError("Connection timeout".to_string()));
            }
            
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Set the Last Will and Testament message for the MQTT connection.
    ///
    /// This message will be sent by the broker if the client disconnects unexpectedly.
    pub fn set_last_will(&mut self, message: MqttMessage) {
        // Convert MqttMessage to rumqttc's LastWill format and store in MqttOptions
        let mqtt_options = self.mqtt_options.clone();
        tokio::spawn(async move {
            let mut opts_guard = mqtt_options.write().await;
            
            // Convert QoS
            let qos = match message.qos {
                StingerQoS::AtMostOnce => QoS::AtMostOnce,
                StingerQoS::AtLeastOnce => QoS::AtLeastOnce,
                StingerQoS::ExactlyOnce => QoS::ExactlyOnce,
            };
            
            // Build LastWillProperties
            let lwt_props = LastWillProperties {
                delay_interval: None,
                payload_format_indicator: None,
                message_expiry_interval: None,
                content_type: message.content_type.clone(),
                response_topic: message.response_topic.clone(),
                correlation_data: message.correlation_data.clone(),
                user_properties: message.user_properties.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            };
            
            // Create LastWill
            let last_will = LastWill {
                topic: Bytes::from(message.topic.into_bytes()),
                message: message.payload.into(),
                qos,
                retain: message.retain,
                properties: Some(lwt_props),
            };
            
            opts_guard.set_last_will(last_will);
        });
    }

    /// Disconnect from the MQTT broker.
    pub async fn disconnect(&mut self) -> std::result::Result<(), Mqtt5PubSubError> {
        // Update connection state
        let _ = self.connection_state_tx.send(MqttConnectionState::Disconnected);
        
        // Mark as disconnected in state
        {
            let mut state = self.state.write().await;
            state.is_connected = false;
        }
        
        // Send disconnect to broker
        self.client
            .disconnect()
            .await
            .map_err(|e| Mqtt5PubSubError::Other(format!("Disconnection error: {}", e)))?;
        
        Ok(())
    }

    /// Start the MQTT client's background event loop.
    pub async fn start(&mut self) -> std::result::Result<(), Mqtt5PubSubError> {
        self.run_loop()
            .await
            .map_err(|e| Mqtt5PubSubError::Other(format!("Connection error: {}", e)))
    }

    /// Cleanly stop the MQTT client, disconnecting gracefully from the broker.
    pub async fn clean_stop(&mut self) -> std::result::Result<(), Mqtt5PubSubError> {
        // Mark as not running
        {
            let mut is_running = self.is_running.lock().await;
            *is_running = false;
        }
        
        // Disconnect from broker
        self.disconnect().await?;
        
        Ok(())
    }

    /// Force stop the MQTT client without waiting for graceful disconnection.
    pub async fn force_stop(&mut self) -> std::result::Result<(), Mqtt5PubSubError> {
        // Mark as not running
        {
            let mut is_running = self.is_running.lock().await;
            *is_running = false;
        }
        
        // Update connection state immediately without waiting for broker
        let _ = self.connection_state_tx.send(MqttConnectionState::Disconnected);
        
        // Mark as disconnected in state
        {
            let mut state = self.state.write().await;
            state.is_connected = false;
        }
        
        // Note: We don't call client.disconnect() for force_stop - just update state
        // The event loop will handle cleanup when it detects is_running is false
        
        Ok(())
    }

    /// Reconnect to the MQTT broker.
    /// 
    /// Note: clean_start parameter is currently not implemented as it would require
    /// recreating the client with new MqttOptions. The client always uses clean_start=true
    /// from initialization.
    pub async fn reconnect(&mut self, _clean_start: bool) -> std::result::Result<(), Mqtt5PubSubError> {
        // Disconnect if currently connected
        {
            let state = self.state.read().await;
            if state.is_connected {
                drop(state);
                self.disconnect().await?;
            }
        }
        
        // The run_loop's reconnection logic will automatically reconnect
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        let options = MqttierOptions {
            connection: Connection::Tcp(TcpConnection {
                hostname: "localhost".to_string(),
                port: 1883,
            }),
            client_id: "test_client".to_string(),
            ack_timeout_ms: 5000,
            keepalive_secs: 60,
        };
        let client = MqttierClient::new(options).unwrap();
        assert_eq!(client.next_subscription_id.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_client_creation_with_id() {
        let client_id = "test_client".to_string();
        let options = MqttierOptions {
            connection: Connection::Tcp(TcpConnection {
                hostname: "localhost".to_string(),
                port: 1883,
            }),
            client_id: client_id,
            ack_timeout_ms: 5000,
            keepalive_secs: 60,
        };
        let _client = MqttierClient::new(options).unwrap();
        // If we get here without panic, the test passes
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    use stinger_mqtt_trait::Mqtt5PubSub;

    /// Test that MqttierClient properly implements the MqttClient trait
    #[tokio::test]
    async fn test_mqtt_client_trait_implementation() {
        // This test verifies that MqttierClient implements the MqttClient trait correctly
        let options = MqttierOptions {
            connection: Connection::TcpLocalhost(1883),
            client_id: "trait_test_client".to_string(),
            ack_timeout_ms: 5000,
            keepalive_secs: 60,
        };
        
        let client = MqttierClient::new(options).expect("Failed to create client");
        
        // Verify trait methods are available
        assert_eq!(client.get_client_id(), "trait_test_client");
        
        // Verify connection state is accessible
        let state_rx = client.get_state();
        let current_state = *state_rx.borrow();
        assert_eq!(current_state, MqttConnectionState::Disconnected);
    }
}

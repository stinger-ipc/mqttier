extern crate paho_mqtt as mqtt;
use uuid::Uuid;
use futures::{StreamExt};
use mqtt::{Message, QOS_1, QOS_2};
use std::collections::HashMap;
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::sync::mpsc::error::SendError;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use serde::Serialize;
use serde_json::{self};
use log::{info, warn, error};


/// Represents a message received from an MQTT subscription, along with its subscription ID.
#[derive(Clone, Debug)]
pub struct ReceivedMessage {
    pub message: mqtt::Message,
    pub subscription_id: i32,
}

/// Streams messages from the MQTT broker and dispatches them to the appropriate subscription channels.
#[derive(Clone)]
pub struct MessageStreamer {
    pub subscriptions: Arc<Mutex<HashMap<i32, Sender<ReceivedMessage>>>>,
    pub strm: mqtt::AsyncReceiver<Option<mqtt::Message>>
}

impl MessageStreamer {
    /// Runs the receive loop, dispatching incoming messages to the correct subscription channel.
    pub async fn receive_loop(&mut self) -> Result<(), paho_mqtt::Error> {
        info!("Starting message receive loop...");
        while let Some(msg_opt) = self.strm.next().await {
            if let Some(msg) = msg_opt {
                let mut sub_id_itr = msg.properties().iter(mqtt::PropertyCode::SubscriptionIdentifier);
                while let Some(sub_id_prop) = sub_id_itr.next() {
                    let opt_sub_id = sub_id_prop.get_int();
                    if let Some(sub_id) = opt_sub_id {
                        if let Some(tx) = self.subscriptions.lock().unwrap().get(&sub_id) {
                            let tx2 = tx.clone();
                            let received_msg = ReceivedMessage {
                                message: msg.clone(),
                                subscription_id: sub_id,
                            };
                            tokio::spawn(async move {
                                if let Err(e) = tx2.send(received_msg).await {
                                    error!("Failed to send message: {}", e);
                                }
                            });
                        } else {
                            warn!("No subscription found for ID: {}", sub_id);
                        } 
                    } else {
                        warn!("No subscription ID found in message properties.");
                    }
                }
            } else {
                info!("Received None, stream closed.");
            }
    
        }
        Ok(())
    }
}

/// Publishes messages to the MQTT broker using an internal channel.
#[derive(Clone)]
pub struct MessagePublisher {
    pub channel: Sender<mqtt::Message>
}

impl MessagePublisher {
    /// Publishes a raw MQTT message.
    pub async fn publish(&mut self, msg: mqtt::Message) -> Result<(), SendError<Message>> {
        info!("Message publisher publishing message to {}", msg.topic());
        self.channel.send(msg).await
    }

    /// Publishes a simple string payload to a topic.
    pub async fn publish_simple(&mut self, topic: String, payload: String) -> Result<(), SendError<Message>> {
        let msg = mqtt::Message::new(topic, payload, QOS_1);
        self.publish(msg).await
    }

    /// Publishes a serializable structure as JSON to a topic.
    pub async fn publish_structure<T: Serialize>(&mut self, topic: String, data: &T) -> Result<(), SendError<Message>> {
        let payload = serde_json::to_string(data).unwrap().into_bytes();
        let mut pub_props = mqtt::Properties::new();
        let _ = pub_props.push_string(mqtt::PropertyCode::ContentType, "application/json");
        let msg = mqtt::MessageBuilder::new()
            .topic(topic)
            .payload(payload)
            .properties(pub_props)
            .qos(QOS_1)
            .finalize();
        self.publish(msg).await
    }

    /// Publishes a retained serializable structure as JSON to a topic.
    pub async fn publish_retained_structure<T: Serialize>(&mut self, topic: String, data: &T) -> Result<(), SendError<Message>> {
        let payload = serde_json::to_string(data).unwrap().into_bytes();
        let mut pub_props = mqtt::Properties::new();
        let _ = pub_props.push_string(mqtt::PropertyCode::ContentType, "application/json");
        let msg = mqtt::MessageBuilder::new()
            .topic(topic)
            .payload(payload)
            .properties(pub_props)
            .qos(QOS_1)
            .retained(true)
            .finalize();
        self.publish(msg).await
    }

    /// Publishes a request structure with response topic and correlation ID for request/response patterns.
    pub async fn publish_request_structure<T: Serialize>(&mut self, topic: String, data: &T, response_topic: &str, correlation_id: Uuid) -> Result<(), SendError<Message>> {
        info!("Publishing request structure to {} with responses going to {}", topic, response_topic);
        let uuid_vec: Vec<u8> = correlation_id.as_bytes().to_vec();
        let mut pub_props = mqtt::Properties::new();
        let _ = pub_props.push_binary(mqtt::PropertyCode::CorrelationData, uuid_vec);
        let _ = pub_props.push_string(mqtt::PropertyCode::ResponseTopic, response_topic);
        let _ = pub_props.push_string(mqtt::PropertyCode::ContentType, "application/json");
        let payload = serde_json::to_string(data).unwrap().into_bytes();
        let msg = mqtt::MessageBuilder::new()
            .topic(topic)
            .qos(QOS_2)
            .properties(pub_props)
            .payload(payload)
            .finalize();
        self.publish(msg).await
    }

    /// Publishes a response structure, optionally including a correlation ID.
    pub async fn publish_response_structure<T: Serialize>(&mut self, topic: String, data: &T, correlation_id: Option<Vec<u8>>) -> Result<(), SendError<Message>> {
        let mut pub_props = mqtt::Properties::new();
        if let Some(corr_id) = correlation_id {
            let _ = pub_props.push_binary(mqtt::PropertyCode::CorrelationData, corr_id);
        }
        let payload = serde_json::to_string(data).unwrap().into_bytes();
        let _ = pub_props.push_string(mqtt::PropertyCode::ContentType, "application/json");
        let msg = mqtt::MessageBuilder::new()
            .topic(topic)
            .payload(payload)
            .qos(QOS_2)
            .properties(pub_props)
            .finalize();
        self.publish(msg).await
    }
}

/// Represents an MQTT connection, including client, subscriptions, and publisher channel.
pub struct Connection {
    pub client: paho_mqtt::AsyncClient,
    pub client_id: String,
    pub next_subscription_id: i32,
    pub subscriptions: Arc<Mutex<HashMap<i32, Sender<ReceivedMessage>>>>,
    pub pub_chan_rx: Receiver<mqtt::Message>,
    pub pub_chan_tx: Sender<mqtt::Message>,
}

impl Connection {
    /// Creates a new MQTT connection to the specified broker.
    pub async fn new(broker: &str) -> Result<Self, paho_mqtt::Error> {
        info!("Creating new connection object");
        let client_id_uuid = Uuid::new_v4();
        let client_id = client_id_uuid.to_string();
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(broker)
            .mqtt_version(mqtt::MQTT_VERSION_5)
            .allow_disconnected_send_at_anytime(true)
            .client_id(client_id.clone())
            .finalize();
        let (tx, rx) = mpsc::channel(32);
        let client = paho_mqtt::AsyncClient::new(create_opts)?;
        Ok(Self { 
            client,
            client_id,
            next_subscription_id: 5,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            pub_chan_rx: rx,
            pub_chan_tx: tx,
        })
    }

    
    /// Creates a new MQTT connection to the default broker (localhost:1883).
    pub async fn new_default_connection() -> Result<Self, paho_mqtt::Error> {
        Connection::new("localhost:1883").await
    }
    

    /// Returns the next available subscription ID.
    fn get_subscription_id(&mut self) -> i32 {
        let id = self.next_subscription_id;
        self.next_subscription_id += 1;
        id
    }

    /// Returns a message streamer for receiving messages.
    pub async fn get_streamer(&mut self) -> MessageStreamer {
        let strm = self.client.get_stream(25);
        MessageStreamer {
            subscriptions: self.subscriptions.clone(),
            strm,
        }
    }

    /// Returns a message publisher for sending messages.
    pub fn get_publisher(&self) -> MessagePublisher {
        let channel = self.pub_chan_tx.clone();
        MessagePublisher {
            channel,
        }
    }

    /// Connects to the MQTT broker.
    pub async fn connect(&self) -> Result<paho_mqtt::ServerResponse, paho_mqtt::Error> {
        info!("Connecting to mqtt");
        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_start(true)
            .finalize();

        self.client.connect(conn_opts).await
    }

    /// Disconnects from the MQTT broker.
    pub async fn disconnect(&self) -> Result<paho_mqtt::ServerResponse, paho_mqtt::Error> {
        self.client.disconnect(None).await
    }
    
    /// Publishes a message directly to the MQTT broker.
    pub async fn publish(&self, msg: mqtt::Message) -> Result<(), paho_mqtt::Error> {
        info!("Publishing message to {}", msg.topic());
        let pub_result = self.client.publish(msg).await;
        pub_result
    }

    /// Adds a subscription receiver channel for a given subscription ID.
    async fn add_subscription_receiver(
        &mut self,
        subscription_id: i32,
        tx: Sender<ReceivedMessage>,
    ) -> Result<(), paho_mqtt::Error> {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        subscriptions.insert(subscription_id, tx);
        Ok(())
    }

    /// Subscribes to a topic and registers a channel for received messages.
    pub async fn subscribe(&mut self, topic: &str, tx: Sender<ReceivedMessage>) -> Result<i32, paho_mqtt::Error> {
        info!("Subscribing to {}", topic);
        let sub_opts = mqtt::SubscribeOptions::new(true, false, mqtt::RetainHandling::SendRetainedOnSubscribe);
        let mut sub_props = mqtt::Properties::new();
        let subscription_id = self.get_subscription_id();
        let si_prop =  mqtt::Property::new(mqtt::PropertyCode::SubscriptionIdentifier, subscription_id as i32).unwrap();
        let _ = sub_props.push(si_prop);
        let _sub_result = self.client.subscribe_with_options(topic, paho_mqtt::QOS_1, sub_opts, sub_props).await;
        self.add_subscription_receiver(subscription_id, tx).await?;
        Ok(subscription_id)
    }

    /// Starts the main MQTT event loop, handling publishing and receiving.
    pub async fn start_loop(&mut self) {
        info!("Starting MQTT loop...");

        let mut streamer = self.get_streamer().await;
        let _stream_task = tokio::spawn(async move {
            let _ = streamer.receive_loop().await;
        });

        loop {
            let opt_msg = self.pub_chan_rx.recv().await;
            if let Some(msg) = opt_msg {
                let pub_result = self.publish(msg).await;
                match pub_result {
                    Ok(_r) => info!("Message was published"),
                    Err(e) => error!("Error publishing: {:?}", e),
                }
            }
        }
    }

}

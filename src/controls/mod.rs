use super::Synchronizer;
use crate::util::controls_to_phoebus;
use rust_pubsub_lib::{
    Message, Publisher, Subscriber,
    kafka_impl::{KafkaPublisher, KafkaSubscriber},
};
use std::{
    collections::{HashMap, HashSet},
    env,
};
use tokio_stream::StreamExt;
use tracing::warn;

pub struct SyncImpl {
    controls: KafkaSubscriber,
    phoebus_devices: HashMap<String, HashSet<String>>,
    phoebus_publishers: HashMap<String, KafkaPublisher>,
}

impl SyncImpl {
    fn find_topic(&self, msg_key: &Option<String>) -> String {
        msg_key
            .as_ref()
            .and_then(|device| {
                self.phoebus_devices
                    .iter()
                    .find(|(_, devices)| devices.contains(device))
                    .map(|(topic, _)| topic.clone())
            })
            .unwrap_or_default()
    }

    fn process_message(&mut self, msg: Message) {
        let topic = self.find_topic(&msg.key);
        match self.phoebus_publishers.get_mut(&topic) {
            Some(publisher) => {
                if let Err(err) = controls_to_phoebus(msg)
                    .map_err(|e| format!("{e:?}"))
                    .and_then(|msg| publisher.publish(msg).map_err(|e| format!("{e:?}")))
                {
                    warn!("Failed publishing to Phoebus Kafka.\n  Cause: {err}");
                }
            }
            None => {
                warn!(
                    "Received message for device with no matching Phoebus topic. Message: {msg:?}"
                );
            }
        }
    }
}

#[async_trait::async_trait]
impl Synchronizer for SyncImpl {
    fn new() -> Self {
        let controls_host =
            env::var("CONTROLS_HOST").expect("CONTROLS_HOST environment variable not set");
        let controls_topic =
            env::var("CONTROLS_TOPIC").expect("CONTROLS_TOPIC environment variable not set");
        let phoebus_host =
            env::var("PHOEBUS_HOST").expect("PHOEBUS_HOST environment variable not set");
        let phoebus_topics: Vec<String> = env::var("PHOEBUS_TOPICS")
            .expect("PHOEBUS_TOPICS environment variable not set")
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        SyncImpl {
            controls: KafkaSubscriber::new(controls_host, controls_topic),
            phoebus_devices: HashMap::new(),
            phoebus_publishers: phoebus_topics
                .into_iter()
                .map(|topic| {
                    (
                        topic.clone(),
                        KafkaPublisher::new(phoebus_host.clone(), topic),
                    )
                })
                .collect(),
        }
    }

    async fn synchronize(&mut self) {
        let mut controls_stream = self.controls.get_stream();
        loop {
            match controls_stream.next().await.unwrap() {
                // Unwrap is safe here because the stream only ends if the consumer dies, in which case the service should terminate.
                Ok(msg) => {
                    self.process_message(msg);
                }
                Err(e) => {
                    warn!(
                        "Error receiving message from Controls Kafka. Attempting to reconnect.\n  Cause: {e:?}"
                    );
                }
            }
        }
    }
}

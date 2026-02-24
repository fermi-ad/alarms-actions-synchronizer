use super::{AlarmStateCache, PvCache, Synchronizer, SynchronizerConfig};
mod transform;
use crate::{
    models::{
        alarm::{
            Status,
            status::{Source, State},
        },
        phoebus::Operation,
    },
    utils::get_command_topic,
};
use rust_pubsub_lib::{
    Message, Publisher, Subscriber,
    kafka_impl::{KafkaPublisher, KafkaSubscriber},
};
use std::collections::HashMap;
use tokio_stream::StreamExt;
use tracing::{debug, error, warn};

pub struct SyncImpl {
    alarms_states: AlarmStateCache,
    controls: KafkaSubscriber,
    phoebus_publishers: HashMap<String, KafkaPublisher>,
    pv_metadata: PvCache,
}

impl SyncImpl {
    async fn process_message(&mut self, msg: Message) {
        let controls_alarm = match serde_json::from_str::<Status>(&msg.value) {
            Ok(alarm) => alarm,
            Err(e) => {
                error!(
                    "Failed to deserialize Controls message value: {e}\n Message value: {}",
                    msg.value
                );
                return;
            }
        };
        let pv_metadata = match self
            .pv_metadata
            .read()
            .await
            .get(&controls_alarm.device)
            .cloned() // Get our own copy so we can drop the reference to the shared cache
        {
            Some(metadata) => metadata,
            None => {
                handle_missing_metadata(controls_alarm);
                return;
            }
        };
        let state_opt = self
            .alarms_states
            .read()
            .await
            .get(&controls_alarm.device)
            .cloned(); // Get our own copy so we can drop the reference to the shared cache
        let cached_state = match state_opt {
            Some(state) => state,
            None => {
                debug!(
                    "No cached alarm state for device {}. Caching state and doing nothing.",
                    controls_alarm.device
                );
                self.alarms_states
                    .write()
                    .await
                    .insert(controls_alarm.device.clone(), controls_alarm.into());
                return;
            }
        };
        if cached_state.state == controls_alarm.state() && cached_state.wake == controls_alarm.wake
        {
            debug!(
                "Received alarm update for device {} with unchanged state {cached_state:?}. Doing nothing.",
                controls_alarm.device
            );
            return;
        }
        let operation = match controls_alarm.state() {
            State::Acknowledged => Operation::Command,
            State::Bypassed => Operation::Config,
            _ => {
                debug!(
                    "Received alarm update for device {} with new state {:?} that does not require synchronization. Updating cache and doing nothing.",
                    controls_alarm.device,
                    controls_alarm.state()
                );
                self.alarms_states
                    .write()
                    .await
                    .insert(controls_alarm.device.clone(), controls_alarm.into());
                return;
            }
        };
        let (topic, message) = match transform::controls_to_phoebus(
            &controls_alarm,
            operation,
            &pv_metadata,
        ) {
            Ok(topic_and_message) => topic_and_message,
            Err(err) => {
                error!(
                    "Unable to create message to send to Phoebus.\n Cause: {err}\n Message from Controls: {controls_alarm:?}"
                );
                return;
            }
        };
        match self.phoebus_publishers.get_mut(&topic) {
            Some(publisher) => {
                if let Err(err) = publisher.publish(message) {
                    warn!(
                        "Failed publishing to Phoebus Kafka.\n  Cause: {err}\n Message from Controls: {controls_alarm:?}"
                    );
                }
            }
            None => {
                warn!(
                    "Received message for device with no matching Phoebus topic. Message: {msg:?}"
                );
            }
        };
        self.alarms_states
            .write()
            .await
            .insert(controls_alarm.device.clone(), controls_alarm.into());
    }
}

#[async_trait::async_trait]
impl Synchronizer for SyncImpl {
    fn new(config: SynchronizerConfig) -> Self {
        SyncImpl {
            alarms_states: config.alarm_states,
            controls: KafkaSubscriber::new(config.controls_host, config.controls_topic),
            phoebus_publishers: config
                .phoebus_topics
                .into_iter()
                .flat_map(|topic| {
                    let command_topic = get_command_topic(&topic);
                    [
                        (
                            topic.clone(),
                            KafkaPublisher::new(config.phoebus_host.clone(), topic),
                        ),
                        (
                            command_topic.clone(),
                            KafkaPublisher::new(config.phoebus_host.clone(), command_topic),
                        ),
                    ]
                })
                .collect(),
            pv_metadata: config.pv_metadata,
        }
    }

    async fn synchronize(&mut self) {
        let mut controls_stream = self.controls.get_stream();
        loop {
            match controls_stream.next().await.unwrap() {
                // Unwrap is safe here because the stream only ends if the consumer dies, in which case the service should terminate.
                Ok(msg) => {
                    self.process_message(msg).await;
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

fn handle_missing_metadata(status: Status) {
    match status.source() {
        Source::Epics => warn!(
            "Received message for EPICS device '{}' with no matching PV metadata. This likely means the message is an alarm update for an EPICS PV that the synchronizer has not yet received metadata for from Phoebus. Message will be dropped.",
            status.device
        ),
        Source::Unknown => warn!(
            "Received message for device '{}' with unknown source and no matching PV metadata. This likely means the message is corrupted or some other error occured. Message will be dropped.",
            status.device
        ),
        _ => debug!(
            "Received message for ACNET device '{}'. Doing nothing.",
            status.device
        ),
    }
}

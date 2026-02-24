use super::{AlarmStateCache, PvCache, Synchronizer, SynchronizerConfig};
use crate::{
    models::{
        ACK_COMMAND, CachedState,
        alarm::status::State,
        generated::Timestamp,
        phoebus::{Command, Config, Key, Operation, PvMetadata},
    },
    utils::get_command_topic,
};
use chrono::{DateTime, Utc};
use rust_pubsub_lib::{
    Message, Publisher, Subscriber,
    kafka_impl::{KafkaPublisher, KafkaSubscriber},
};
use std::collections::HashMap;
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose, wrappers::BroadcastStream};
use tracing::{debug, error, info, warn};

pub struct SyncImpl {
    alarm_states: AlarmStateCache,
    _controls: KafkaPublisher, // Swap this out with the gRPC service for talking to the Controls alarms app when it becomes available.
    phoebus: HashMap<String, KafkaSubscriber>,
    pv_metadata: PvCache,
}
impl SyncImpl {
    fn generate_stream_map(
        &self,
    ) -> StreamMap<String, StreamNotifyClose<BroadcastStream<Message>>> {
        self.phoebus
            .iter()
            .map(|(topic, sub)| (topic.clone(), StreamNotifyClose::new(sub.get_stream())))
            .collect::<StreamMap<_, _>>()
    }

    async fn get_pv_metadata(&self, topic: &str, key: &Key) -> PvMetadata {
        let metadata_opt = self.pv_metadata.read().await.get(&key.device).cloned();

        metadata_opt.unwrap_or_else(|| PvMetadata {
            config: Config::default(),
            display_path: key.display_path.clone(),
            phoebus_topic: topic.strip_suffix("Command").unwrap_or(topic).to_owned(),
        })
    }

    async fn handle_active_alarm(&self, key: &Key) {
        let cached_state = self.alarm_states.read().await.get(&key.device).cloned();
        if let Some(state) = cached_state
            && state.state != State::Bypassed
        {
            info!(
                "Received configuration update from Phoebus to activate alarm for device '{}', but it is already active. Updating cached config only.",
                key.device
            );
            return;
        }
        info!(
            "TODO: Update the alarms serivce with an Ok condition for device '{}'",
            key.device
        );
        self.alarm_states.write().await.insert(
            key.device.clone(),
            CachedState {
                state: State::Ok,
                wake: None,
            },
        );
    }

    async fn handle_bypassed_alarm(&self, key: &Key, wake: Option<Timestamp>) {
        let cached_state = self.alarm_states.read().await.get(&key.device).cloned();
        if let Some(state) = cached_state
            && state.state == State::Bypassed
            && state.wake == wake
        {
            info!(
                "Received configuration update from Phoebus to bypass alarm for device '{}', but it is already bypassed. Updating cached PV config only.",
                key.device
            );
            return;
        }
        match wake {
            Some(time) => {
                info!(
                    "TODO: Update the alarms serivce with a Snoozed condition for device '{}' with wake time '{:?}'",
                    key.device, time
                );
            }
            None => {
                info!(
                    "TODO: Update the alarms serivce with a Bypassed condition for device '{}'",
                    key.device
                );
            }
        }
        self.alarm_states.write().await.insert(
            key.device.clone(),
            CachedState {
                state: State::Bypassed,
                wake,
            },
        );
    }

    async fn process_command(&self, key: Key, msg_text: String) {
        let command_msg = match serde_json::from_str::<Command>(&msg_text) {
            Ok(inner) => inner,
            Err(e) => {
                error!(
                    "Failed to deserialize Phoebus command: {e}\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
                );
                return;
            }
        };
        if command_msg.command != ACK_COMMAND {
            debug!(
                "Received Phoebus command that does not need to be processed. Doing nothing.\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
            );
            return;
        }
        if let Some(cur_state) = self.alarm_states.read().await.get(&key.device)
            && cur_state.state == State::Acknowledged
        {
            info!(
                "Received acknowledgement command from Phoebus for device '{}', but it is already acknowledged. Doing nothing.",
                key.device
            );
            return;
        }
        info!(
            "TODO: make call to Controls alarms service, indicating that device '{}' has been acknowledged.",
            key.device
        );
        self.alarm_states.write().await.insert(
            key.device,
            CachedState {
                state: State::Acknowledged,
                wake: None,
            },
        );
    }

    async fn process_config(&self, topic: &str, key: Key, msg_text: String) {
        let config_msg = match serde_json::from_str::<Config>(&msg_text) {
            Ok(inner) => inner,
            Err(e) => {
                error!(
                    "Failed to deserialize Phoebus config: {e}\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
                );
                return;
            }
        };
        let cached_metadata = self.get_pv_metadata(topic, &key).await;
        if config_msg == cached_metadata.config {
            info!(
                "Received config from Phoebus for device '{}' that matches the cached config. Doing nothing.",
                key.device
            );
            return;
        }
        if config_msg.enabled != cached_metadata.config.enabled {
            match config_msg.enabled.as_ref() {
                Some(enabled_str) => match DateTime::parse_from_rfc3339(enabled_str) {
                    Ok(dt) => {
                        if dt.timestamp_millis() > Utc::now().timestamp_millis() {
                            self.handle_bypassed_alarm(
                                &key,
                                Some(Timestamp {
                                    seconds: dt.timestamp(),
                                    nanos: dt.timestamp_subsec_nanos() as i32,
                                }),
                            )
                            .await;
                        } else {
                            self.handle_active_alarm(&key).await;
                        }
                    }
                    Err(_) => {
                        if let Ok(is_active) = enabled_str.parse::<bool>() {
                            if is_active {
                                self.handle_active_alarm(&key).await;
                            } else {
                                self.handle_bypassed_alarm(&key, None).await;
                            }
                        } else {
                            error!(
                                "Could not parse the enabled state of a Phoebus config message to either a date or a bool.\n Device: {}\n Config record: {:?}",
                                key.device, config_msg
                            );
                        }
                    }
                },
                None => {
                    self.handle_bypassed_alarm(&key, None).await;
                }
            }
        }
        self.pv_metadata.write().await.insert(
            key.device.clone(),
            PvMetadata {
                config: config_msg,
                ..cached_metadata
            },
        );
    }

    async fn process_message(&self, topic: &str, msg: Message) {
        if msg.key.is_none() {
            error!(
                "Got message with no key. There is a problem with the pub-sub crate or with the messages in the Phoebus Kafka.\n Message: {msg:?}"
            );
            return;
        }
        let key = Key::from(msg.key.unwrap());
        match key.operation {
            Operation::Command => {
                self.process_command(key, msg.value).await;
            }
            Operation::Config => {
                self.process_config(topic, key, msg.value).await;
            }
            Operation::Other => debug!(
                "Received Phoebus message that is not a config or a command. Doing nothing.\n Original message from Phoebus: {{ key: {key:?}, text: {} }}",
                msg.value
            ),
        }
    }
}
#[async_trait::async_trait]
impl Synchronizer for SyncImpl {
    fn new(config: SynchronizerConfig) -> Self {
        SyncImpl {
            alarm_states: config.alarm_states,
            _controls: KafkaPublisher::new(config.controls_host, config.controls_topic),
            phoebus: config
                .phoebus_topics
                .into_iter()
                .flat_map(|topic| {
                    let command_topic = get_command_topic(&topic);
                    [
                        (
                            command_topic.clone(),
                            KafkaSubscriber::new(config.phoebus_host.clone(), command_topic),
                        ),
                        (
                            topic.clone(),
                            KafkaSubscriber::new(config.phoebus_host.clone(), topic),
                        ),
                    ]
                })
                .collect(),
            pv_metadata: config.pv_metadata,
        }
    }

    async fn synchronize(&mut self) {
        let mut stream_map = self.generate_stream_map();
        loop {
            let stream_opt = stream_map.next().await;
            if stream_opt.is_none() {
                // All of the streams closed themselves. Regenerate the map.
                stream_map = self.generate_stream_map();
                continue;
            }
            let (topic, msg_opt) = stream_opt.unwrap();
            if msg_opt.is_none() {
                // One of the streams closed itself. Regenerate the stream.
                let new_stream =
                    StreamNotifyClose::new(self.phoebus.get(&topic).unwrap().get_stream());
                stream_map.insert(topic, new_stream);
                continue;
            }

            match msg_opt.unwrap() {
                Ok(msg) => {
                    self.process_message(&topic, msg).await;
                }
                Err(e) => {
                    warn!(
                        "Error with the internal Kafka Consumer stream. Reconnect in progress.\n Cause: {e:?}"
                    );
                }
            }
        }
    }
}

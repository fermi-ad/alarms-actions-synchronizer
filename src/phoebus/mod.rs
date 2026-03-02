//! Phoebus Module
//!
//! Contains the [`Synchronizer`] for pushing Phoebus commands and configs into the Controls alarm server.

use crate::{
    models::{
        ACK_COMMAND, AlarmStateCache, CachedState, PvCache, Synchronizer, SynchronizerConfig,
        alarm::status::State,
        phoebus::{Command, Config, Key, Operation, PvMetadata},
    },
    utils::get_command_topic,
};
use init::get_existing_messages_from_phoebus;
use rust_pubsub_lib::{Message, Publisher, Snapshot, Subscriber};
use std::collections::HashMap;
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose, wrappers::BroadcastStream};
use tracing::{debug, error, info, warn};

mod init;

/// Implementation of [`Synchronizer`] for pushing Phoebus commands and configs into the Controls alarm service.
pub struct SyncImpl<S: Subscriber> {
    /// The atomic cache of alarm state data.
    alarm_states: AlarmStateCache,

    /// The client for passing alarms info to the Controls alarms service.
    _controls: (), // TODO: Swap this out with the gRPC service for talking to the Controls alarms app when it becomes available.

    /// The location of the Phoebus alarms topics. Used during inital startup of the sync operation.
    phoebus_host: String,

    /// The map of [`Subscriber`] instances and their associated topics in the Phoebus Kafka.
    phoebus_subscribers: HashMap<String, S>,

    /// The atomic cache of PV metadata.
    pv_metadata: PvCache,
}
impl<S: Subscriber> SyncImpl<S> {
    /// (Re)Generates the map of streams from the Phoebus subscribers.
    fn generate_stream_map(
        &self,
    ) -> StreamMap<String, StreamNotifyClose<BroadcastStream<Message>>> {
        self.phoebus_subscribers
            .iter()
            .map(|(topic, sub)| (topic.clone(), StreamNotifyClose::new(sub.get_stream())))
            .collect::<StreamMap<_, _>>()
    }

    /// Looks up the corresponding [`PvMetadata`] for a given [`Key`], or creates one if none exists.
    async fn get_pv_metadata(&self, topic: &str, key: &Key) -> PvMetadata {
        let metadata_opt = self.pv_metadata.read().await.get(&key.device).cloned();

        metadata_opt.unwrap_or_else(|| PvMetadata {
            config: Config::default(),
            display_path: key.display_path.clone(),
            phoebus_topic: topic.strip_suffix("Command").unwrap_or(topic).to_owned(),
        })
    }

    /// Handles when a config came in from Phoebus to activate a bypassed alarm.
    async fn handle_active_alarm(&self, device: &str, updated_state: CachedState) {
        let cached_state = self.alarm_states.read().await.get(device).cloned();
        if let Some(state) = cached_state
            && state.state != State::Bypassed
        {
            info!(
                "Received configuration update from Phoebus to activate alarm for device '{}', but it is already active. Updating cached config only.",
                device
            );
            return;
        }
        info!(
            "TODO: Update the alarms serivce with an Ok condition for device '{}'",
            device
        );
        self.alarm_states
            .write()
            .await
            .insert(device.to_owned(), updated_state);
    }

    /// Handles when a config came in from Phoebus to bypass an active alarm.
    async fn handle_bypassed_alarm(&self, device: &str, updated_state: CachedState) {
        let cached_state = self.alarm_states.read().await.get(device).cloned();
        if let Some(state) = cached_state
            && state == updated_state
        {
            info!(
                "Received configuration update from Phoebus to bypass alarm for device '{}', but it is already bypassed. Updating cached PV config only.",
                device
            );
            return;
        }
        match updated_state.wake {
            Some(time) => {
                info!(
                    "TODO: Update the alarms serivce with a Snoozed condition for device '{}' with wake time '{:?}'",
                    device, time
                );
            }
            None => {
                info!(
                    "TODO: Update the alarms serivce with a Bypassed condition for device '{}'",
                    device
                );
            }
        }
        self.alarm_states
            .write()
            .await
            .insert(device.to_owned(), updated_state);
    }

    /// Handles a Command message coming in from Phoebus.
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

    /// Handles a message from Phoebus that updates the configuration of a PV.
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
            let updated_state = config_msg.as_cached_state();
            if updated_state.state == State::Bypassed {
                self.handle_bypassed_alarm(&key.device, updated_state).await;
            } else if updated_state.state == State::Ok {
                self.handle_active_alarm(&key.device, updated_state).await;
            } else {
                error!("Could not determine state of new config message: {config_msg:?}");
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

    /// The primary logic for handling a new message from Phoebus.
    /// Disambiguates the type of message and hands it off to the appropriate helper method.
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
impl<P: Publisher, S: Subscriber + Send + Sync> Synchronizer<P, S> for SyncImpl<S> {
    fn new(config: SynchronizerConfig) -> Self {
        SyncImpl {
            alarm_states: config.alarm_states,
            _controls: (),
            phoebus_host: config.phoebus_host.clone(),
            phoebus_subscribers: config
                .phoebus_topics
                .into_iter()
                .flat_map(|topic| {
                    let command_topic = get_command_topic(&topic);
                    [
                        (
                            command_topic.clone(),
                            S::new(config.phoebus_host.clone(), command_topic),
                        ),
                        (topic.clone(), S::new(config.phoebus_host.clone(), topic)),
                    ]
                })
                .collect(),
            pv_metadata: config.pv_metadata,
        }
    }

    async fn synchronize<SNAP: Snapshot>(&mut self) {
        info!("Starting Phoebus-to-Controls Synchronizer");
        let mut stream_map = self.generate_stream_map();
        get_existing_messages_from_phoebus::<SNAP>(
            self.phoebus_host.clone(),
            self.phoebus_subscribers.keys().cloned().collect(),
            &self.alarm_states,
            &self.pv_metadata,
        )
        .await;
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
                let new_stream = StreamNotifyClose::new(
                    self.phoebus_subscribers.get(&topic).unwrap().get_stream(),
                );
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::utils::testing::{
        TestInstance, TestPublisher, TestSubscriber, get_mock_sync_config,
    };
    use chrono::Utc;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::broadcast::Sender;

    fn get_sender(sync: &SyncImpl<TestSubscriber>) -> Sender<Message> {
        sync.phoebus_subscribers
            .get("testTopicCommand")
            .unwrap()
            .sender
            .clone()
    }

    fn get_test_objects() -> (SyncImpl<TestSubscriber>, Sender<Message>) {
        let sync: SyncImpl<TestSubscriber> =
            Synchronizer::<TestPublisher, TestSubscriber>::new(get_mock_sync_config());
        let sender = get_sender(&sync);
        (sync, sender)
    }

    #[test]
    fn should_create_new_synchronizer() {
        let sync: SyncImpl<TestSubscriber> =
            Synchronizer::<TestPublisher, TestSubscriber>::new(get_mock_sync_config());
        assert_eq!((), sync._controls);
        assert!(sync.phoebus_subscribers.contains_key("testTopic"));
        assert!(sync.phoebus_subscribers.contains_key("testTopicCommand"));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_messages_without_keys() {
        let (sync, sender) = get_test_objects();

        let message = Message {
            key: None,
            value: String::new(),
        };

        TestInstance::check_that(sync).when(sender, message).satisfies(async || {
                logs_contain(
                    "Got message with no key. There is a problem with the pub-sub crate or with the messages in the Phoebus Kafka.",
                )
            })
            .await
            .expect("The expected log message was never recorded");
    }

    #[tokio::test]
    async fn should_sync_valid_acknowledge_commands() {
        let (sync, sender) = get_test_objects();

        let alarm_states = Arc::clone(&sync.alarm_states);

        let command = Command {
            user: String::from("my user"),
            host: String::new(),
            command: ACK_COMMAND.to_string(),
        };
        let message = Message {
            key: Some(String::from("command:my/path/to/MyDevice")),
            value: serde_json::to_string(&command).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                alarm_states
                    .read()
                    .await
                    .get("MyDevice")
                    .is_some_and(|state| state.state == State::Acknowledged)
            })
            .await
            .expect("The alarm state was not set to 'Acknowledged'");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_unparseable_command_messages() {
        let (sync, sender) = get_test_objects();

        let message = Message {
            key: Some(String::from("command:not/a/real/Device")),
            value: String::from("{ \"fakeKey\": \"Can't be parsed to a Command object\" }"),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Failed to deserialize Phoebus command"))
            .await
            .expect("The expected log message was never recorded");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_invalid_commands() {
        let (sync, sender) = get_test_objects();

        let command = Command {
            user: String::from("my user"),
            host: String::new(),
            command: String::from("some other command"),
        };
        let message = Message {
            key: Some(String::from("command:my/path/to/MyDevice")),
            value: serde_json::to_string(&command).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain(
                    "Received Phoebus command that does not need to be processed. Doing nothing.",
                )
            })
            .await
            .expect("The expected log message was never recorded");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_already_synced_commands() {
        let (sync, sender) = get_test_objects();

        sync.alarm_states.write().await.insert(
            String::from("MyDevice"),
            CachedState {
                state: State::Acknowledged,
                wake: None,
            },
        );

        let command = Command {
            user: String::from("my user"),
            host: String::new(),
            command: ACK_COMMAND.to_string(),
        };
        let message = Message {
            key: Some(String::from("command:my/path/to/MyDevice")),
            value: serde_json::to_string(&command).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain(
                    "Received acknowledgement command from Phoebus for device 'MyDevice', but it is already acknowledged. Doing nothing.",
                )
            })
            .await
            .expect("The expected log message was never recorded");
    }

    #[tokio::test]
    async fn should_sync_valid_bypass_config() {
        let (sync_with_none, sender) = get_test_objects();

        let sync_with_false = SyncImpl {
            alarm_states: Arc::clone(&sync_with_none.alarm_states),
            _controls: (),
            phoebus_host: String::new(),
            phoebus_subscribers: sync_with_none.phoebus_subscribers.clone(),
            pv_metadata: Arc::clone(&sync_with_none.pv_metadata),
        };

        let alarm_states = Arc::clone(&sync_with_none.alarm_states);

        let mut config = Config::default();
        config.enabled = Some(true.to_string());

        sync_with_none.pv_metadata.write().await.insert(
            String::from("MyDevice"),
            PvMetadata {
                config: config.clone(),
                display_path: String::from("my/path/to"),
                phoebus_topic: String::from("testTopic"),
            },
        );

        config.enabled = None;

        let message = Message {
            key: Some(String::from("config:my/path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };

        TestInstance::check_that(sync_with_none)
            .when(sender, message)
            .satisfies(async || {
                alarm_states
                    .read()
                    .await
                    .get("MyDevice")
                    .is_some_and(|state| state.state == State::Bypassed)
            })
            .await
            .expect("The alarm state was not set to 'Bypassed'");

        config.enabled = Some(false.to_string());
        let message = Message {
            key: Some(String::from("config:my/path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };
        alarm_states.write().await.insert(
            String::from("MyDevice"),
            CachedState {
                state: State::Ok,
                wake: None,
            },
        );
        config.enabled = Some(true.to_string());
        sync_with_false.pv_metadata.write().await.insert(
            String::from("MyDevice"),
            PvMetadata {
                config: config.clone(),
                display_path: String::from("my/path/to"),
                phoebus_topic: String::from("testTopic"),
            },
        );

        let sender = get_sender(&sync_with_false);

        TestInstance::check_that(sync_with_false)
            .when(sender, message)
            .satisfies(async || {
                alarm_states
                    .read()
                    .await
                    .get("MyDevice")
                    .is_some_and(|state| state.state == State::Bypassed)
            })
            .await
            .expect("The alarm state was not set to 'Bypassed'");
    }

    #[tokio::test]
    async fn should_sync_valid_snooze_config() {
        let (sync, sender) = get_test_objects();

        let alarm_states = Arc::clone(&sync.alarm_states);

        let mut config = Config::default();
        config.enabled = Some(true.to_string());

        sync.pv_metadata.write().await.insert(
            String::from("MyDevice"),
            PvMetadata {
                config: config.clone(),
                display_path: String::from("my/path/to"),
                phoebus_topic: String::from("testTopic"),
            },
        );

        config.enabled = Some((Utc::now() + Duration::from_hours(1)).to_rfc3339());

        let message = Message {
            key: Some(String::from("config:my/path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                alarm_states
                    .read()
                    .await
                    .get("MyDevice")
                    .is_some_and(|state| state.state == State::Bypassed && state.wake.is_some())
            })
            .await
            .expect(
                "The alarm state was not set to 'Bypassed' or the state's wake value was not set",
            );
    }

    #[tokio::test]
    async fn should_sync_valid_active_config() {
        let (sync_with_time, sender) = get_test_objects();

        let sync_with_true = SyncImpl {
            alarm_states: Arc::clone(&sync_with_time.alarm_states),
            _controls: (),
            phoebus_host: String::new(),
            phoebus_subscribers: sync_with_time.phoebus_subscribers.clone(),
            pv_metadata: Arc::clone(&sync_with_time.pv_metadata),
        };

        let alarm_states = Arc::clone(&sync_with_time.alarm_states);

        sync_with_time.pv_metadata.write().await.insert(
            String::from("MyDevice"),
            PvMetadata {
                config: Config::default(),
                display_path: String::from("my/path/to"),
                phoebus_topic: String::from("testTopic"),
            },
        );

        let mut config = Config::default();
        config.enabled = Some((Utc::now() - Duration::from_hours(1)).to_rfc3339());

        let message = Message {
            key: Some(String::from("config:my/path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };

        TestInstance::check_that(sync_with_time)
            .when(sender, message)
            .satisfies(async || {
                alarm_states
                    .read()
                    .await
                    .get("MyDevice")
                    .is_some_and(|state| state.state == State::Ok)
            })
            .await
            .expect("The alarm state was not set to 'Ok'");

        config.enabled = Some(true.to_string());
        let message = Message {
            key: Some(String::from("config:my/path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };
        alarm_states.write().await.insert(
            String::from("MyDevice"),
            CachedState {
                state: State::Bypassed,
                wake: None,
            },
        );
        config.enabled = None;

        let sender = get_sender(&sync_with_true);

        // Tests when there's no previous config cached - defaults to an inactive state
        sync_with_true.pv_metadata.write().await.remove("MyDevice");

        TestInstance::check_that(sync_with_true)
            .when(sender, message)
            .satisfies(async || {
                alarm_states
                    .read()
                    .await
                    .get("MyDevice")
                    .is_some_and(|state| state.state == State::Ok)
            })
            .await
            .expect("The alarm state was not set to 'Ok'");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_corrupted_config() {
        let (sync, sender) = get_test_objects();
        let message = Message {
            key: Some(String::from("config:path/to/MyDevice")),
            value: String::from("{ \"notRealConfigMessage\": \"Should not parse\" }"),
        };
        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Failed to deserialize Phoebus config"))
            .await
            .expect("The expected log message was not detected.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_duplicated_config() {
        let (sync, sender) = get_test_objects();

        let config = Config::default();

        sync.pv_metadata.write().await.insert(
            String::from("MyDevice"),
            PvMetadata {
                config: config.clone(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        );

        let message = Message {
            key: Some(String::from("config:path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };
        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Received config from Phoebus for device 'MyDevice' that matches the cached config. Doing nothing."))
            .await
            .expect("The expected log message was not detected.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_unexpected_enabled_states() {
        let (sync, sender) = get_test_objects();

        let mut config = Config::default();

        sync.pv_metadata.write().await.insert(
            String::from("MyDevice"),
            PvMetadata {
                config: config.clone(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        );

        config.enabled = Some(String::from("invalid value"));

        let message = Message {
            key: Some(String::from("config:path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };
        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Could not parse the enabled state of a Phoebus config message to either a date or a bool."))
            .await
            .expect("The expected log message was not detected.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_already_active_config() {
        let (sync, sender) = get_test_objects();

        let mut config = Config::default();

        sync.pv_metadata.write().await.insert(
            String::from("MyDevice"),
            PvMetadata {
                config: config.clone(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        );
        sync.alarm_states.write().await.insert(
            String::from("MyDevice"),
            CachedState {
                state: State::Ok,
                wake: None,
            },
        );

        config.enabled = Some(true.to_string());

        let message = Message {
            key: Some(String::from("config:path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };
        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Received configuration update from Phoebus to activate alarm for device 'MyDevice', but it is already active. Updating cached config only."))
            .await
            .expect("The expected log message was not detected.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_already_bypassed_config() {
        let (sync, sender) = get_test_objects();

        let mut config = Config::default();
        config.enabled = Some(true.to_string());

        sync.pv_metadata.write().await.insert(
            String::from("MyDevice"),
            PvMetadata {
                config: config.clone(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        );
        sync.alarm_states.write().await.insert(
            String::from("MyDevice"),
            CachedState {
                state: State::Bypassed,
                wake: None,
            },
        );

        config.enabled = Some(false.to_string());

        let message = Message {
            key: Some(String::from("config:path/to/MyDevice")),
            value: serde_json::to_string(&config).unwrap(),
        };
        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Received configuration update from Phoebus to bypass alarm for device 'MyDevice', but it is already bypassed. Updating cached PV config only."))
            .await
            .expect("The expected log message was not detected.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_unknown_operations() {
        let (sync, sender) = get_test_objects();
        let message = Message {
            key: Some(String::from("some-other-command:path/to/MyDevice")),
            value: String::new(),
        };
        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain(
                    "Received Phoebus message that is not a config or a command. Doing nothing.",
                )
            })
            .await
            .expect("The expected log message was not detected.");
    }
}

//! Controls Module
//!
//! Contains the [`Synchronizer`] for pushing Controls commands and configs into the Phoebus alarm server.

use crate::{
    models::{
        AlarmStateCache, PvCache, Synchronizer, SynchronizerConfig,
        alarm::{Status, status::Source},
        phoebus::{Operation, PvMetadata},
    },
    utils::get_command_topic,
};
use rust_pubsub_lib::{Message, Publisher, Subscriber};
use std::collections::HashMap;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

mod transform;

/// Implementation of [`Synchronizer`] for pushing Controls commands and configs into the Phoebus alarm service.
pub struct SyncImpl<P: Publisher, S: Subscriber> {
    /// The atomic cache of alarm state data.
    alarms_states: AlarmStateCache,

    /// The [`Subscriber`] listening to the Controls Kafka that passes new messages into the sync handler.
    controls: S,

    /// The map of [`Publisher`] instances and their associated topics for use when sending updates to Phoebus.
    phoebus_publishers: HashMap<String, P>,

    /// The atomic cache of PV metadata.
    pv_metadata: PvCache,
}
impl<P: Publisher, S: Subscriber> SyncImpl<P, S> {
    /// Determines whether the cached state of the `controls_alarm` is current
    async fn check_cache_is_not_stale(&self, controls_alarm: &Status) -> bool {
        self.alarms_states
            .read()
            .await
            .get(&controls_alarm.device)
            .is_some_and(|cached_state| {
                cached_state.state == controls_alarm.state()
                    && cached_state.wake == controls_alarm.wake
            })
    }

    /// Extracts the PV metadata record for the provided `device`
    async fn get_pv_metadata(&self, device: &str) -> Option<PvMetadata> {
        self.pv_metadata.read().await.get(device).cloned() // Get our own copy so we can drop the reference to the shared cache
    }

    /// The path to follow when the operation for the `controls_alarm` is one that does not reqiure synchronization.
    /// Simply logs a debug record and updates the cache.
    async fn handle_non_sync_operation(&self, controls_alarm: &Status) {
        debug!(
            "Received Controls alarm update for device {} with new state {:?} that does not require synchronization. Updating cache and doing nothing.",
            controls_alarm.device,
            controls_alarm.state()
        );
        self.update_cache(controls_alarm).await;
    }

    /// Reusable logic for updating the [`CachedState`](crate::models::CachedState) value of the `controls_alarm`.
    async fn update_cache(&self, controls_alarm: &Status) {
        self.alarms_states.write().await.insert(
            controls_alarm.device.clone(),
            controls_alarm.to_owned().into(),
        );
    }

    /// The general steps for processing an EPICS device with updated state.
    async fn process_epics_message(&mut self, controls_alarm: &Status) {
        let operation = transform::state_to_operation(controls_alarm.state());
        if operation == Operation::Other {
            self.handle_non_sync_operation(controls_alarm).await;
            return;
        }

        let pv_metadata = match self.get_pv_metadata(&controls_alarm.device).await {
            Some(metadata) => metadata,
            None => {
                handle_missing_metadata(&controls_alarm.device);
                return;
            }
        };

        let topic = match transform::get_topic_for_operation(&operation, &pv_metadata) {
            Some(topic) => topic,
            None => {
                error!(
                    "Could not find a relevant topic for operation '{operation:?}'.\n Message from Controls: {controls_alarm:?}"
                );
                return;
            }
        };

        let message = match transform::controls_to_phoebus(&controls_alarm, operation, &pv_metadata)
        {
            Ok(message) => message,
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
                    "Received message for device with no matching Phoebus topic.\n Desired topic: {topic}\n Device: {}",
                    controls_alarm.device
                );
            }
        };
    }

    /// Consumes a [`Message`] and determines whether & where an update should be sent to Phoebus.
    async fn process_message(&mut self, msg: Message) -> Result<(), ()> {
        let controls_alarm = deserialize_status(&msg)?;
        if self.check_cache_is_not_stale(&controls_alarm).await {
            handle_not_stale_cached_value(controls_alarm);
            return Ok(());
        }
        if controls_alarm.source() == Source::Epics {
            self.process_epics_message(&controls_alarm).await;
        }
        self.update_cache(&controls_alarm).await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<P: Publisher + Send + Sync, S: Subscriber + Send + Sync> Synchronizer<P, S>
    for SyncImpl<P, S>
{
    fn new(config: SynchronizerConfig) -> Self {
        SyncImpl {
            alarms_states: config.alarm_states,
            controls: S::new(config.controls_host, config.controls_topic),
            phoebus_publishers: config
                .phoebus_topics
                .into_iter()
                .flat_map(|topic| {
                    let command_topic = get_command_topic(&topic);
                    [
                        (topic.clone(), P::new(config.phoebus_host.clone(), topic)),
                        (
                            command_topic.clone(),
                            P::new(config.phoebus_host.clone(), command_topic),
                        ),
                    ]
                })
                .collect(),
            pv_metadata: config.pv_metadata,
        }
    }

    async fn synchronize(&mut self) {
        info!("Starting Controls-to-Phoebus Synchronizer");
        let mut controls_stream = self.controls.get_stream();
        loop {
            match controls_stream.next().await.unwrap() {
                // Unwrap is safe here because the stream only ends if the consumer dies, in which case the service should terminate.
                Ok(msg) => {
                    let _ = self.process_message(msg).await;
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

/// The logic for transforming the value of the provided [`Message`] into a [`Status`].
fn deserialize_status(msg: &Message) -> Result<Status, ()> {
    serde_json::from_str::<Status>(&msg.value).map_err(|e| {
        error!(
            "Failed to deserialize Controls message value: {e}\n Message value: {}",
            msg.value
        )
    })
}

/// Logs a warning that the provided EPICS device does not have any cached PV metadata, so could not be synced to Phoebus.
fn handle_missing_metadata(device: &str) {
    warn!(
        "Received message for EPICS device '{device}' with no matching PV metadata. This likely means the message is an alarm update for an EPICS PV that the synchronizer has not yet received metadata for from Phoebus. Message will be dropped."
    );
}

/// Logs a message that the `controls_alarm` state is already up to date, so no action will be taken.
fn handle_not_stale_cached_value(controls_alarm: Status) {
    debug!(
        "Received alarm update for device {} with unchanged state '{:?}'. Doing nothing.",
        controls_alarm.device,
        controls_alarm.state()
    );
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        models::{
            ACK_COMMAND,
            alarm::status::State,
            phoebus::{Command, Config, PvMetadata},
        },
        utils::testing::{TestInstance, TestPublisher, TestSubscriber, get_mock_sync_config},
    };
    use std::sync::Arc;
    use tokio::sync::broadcast::{Receiver, Sender};

    fn get_sender(sync: &SyncImpl<TestPublisher, TestSubscriber>) -> Sender<Message> {
        sync.controls.sender.clone()
    }

    fn get_receivers(
        sync: &SyncImpl<TestPublisher, TestSubscriber>,
    ) -> HashMap<String, Receiver<Message>> {
        sync.phoebus_publishers
            .iter()
            .map(|(topic, publisher)| (topic.clone(), publisher.get_receiver()))
            .collect()
    }

    fn get_test_objects() -> (
        SyncImpl<TestPublisher, TestSubscriber>,
        Sender<Message>,
        HashMap<String, Receiver<Message>>,
    ) {
        let sync = SyncImpl::new(get_mock_sync_config());
        let sender = get_sender(&sync);
        let receivers = get_receivers(&sync);
        (sync, sender, receivers)
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_corrupted_controls_message() {
        let (sync, sender, _) = get_test_objects();
        let message = Message {
            key: None,
            value: String::from("{ \"unknownKey\": \"Malformed message\" }"),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Failed to deserialize Controls message value"))
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    async fn should_not_transmit_acnet_device() {
        let (sync, sender, _) = get_test_objects();

        let mut status = Status::default();
        status.set_source(Source::Analog);

        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        let cache = Arc::clone(&sync.alarms_states);

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || cache.read().await.contains_key(""))
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    async fn should_not_transmit_unknown_device() {
        let (sync, sender, _) = get_test_objects();

        let status = Status::default();
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        let cache = Arc::clone(&sync.alarms_states);

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || cache.read().await.contains_key(""))
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_unmapped_epics_pv() {
        let (sync, sender, _) = get_test_objects();

        let mut status = Status::default();
        status.set_state(State::Bypassed);
        status.set_source(Source::Epics);
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        status.set_state(State::Alarmed);
        sync.alarms_states
            .write()
            .await
            .insert(String::new(), status.clone().into());

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain("Received message for EPICS device '' with no matching PV metadata.")
            })
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    async fn should_continue_when_no_cached_alarm_state() {
        let (sync, sender, mut receivers) = get_test_objects();

        sync.pv_metadata.write().await.insert(
            String::new(),
            PvMetadata {
                config: Config::default(),
                display_path: String::new(),
                phoebus_topic: String::from("testTopic"),
            },
        );

        let mut status = Status::default();
        status.set_source(Source::Epics);
        status.set_state(State::Acknowledged);
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        let receiver = receivers.get_mut("testTopicCommand").unwrap();

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                receiver
                    .recv()
                    .await
                    .is_ok_and(|msg| msg.key.is_some_and(|k| k == "command:/"))
            })
            .await
            .expect("Did not receive expected message");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_when_no_change_in_alarm_state() {
        let (sync, sender, _) = get_test_objects();

        sync.pv_metadata.write().await.insert(
            String::new(),
            PvMetadata {
                config: Config::default(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        );

        let mut status = Status::default();
        status.set_source(Source::Epics);
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        sync.alarms_states
            .write()
            .await
            .insert(String::new(), status.into());

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain("Received alarm update for device  with unchanged state 'Unknown'. Doing nothing.")
            })
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_when_alarm_state_is_not_syncable() {
        let (sync, sender, _) = get_test_objects();

        sync.pv_metadata.write().await.insert(
            String::new(),
            PvMetadata {
                config: Config::default(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        );

        let mut status = Status::default();
        status.set_source(Source::Epics);

        sync.alarms_states
            .write()
            .await
            .insert(String::new(), status.clone().into());

        status.set_state(State::Ok);
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain("Received Controls alarm update for device  with new state Ok that does not require synchronization. Updating cache and doing nothing.")
            })
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_when_no_publisher_for_topic() {
        let (sync, sender, _) = get_test_objects();

        sync.pv_metadata.write().await.insert(
            String::new(),
            PvMetadata {
                config: Config::default(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        );

        let mut status = Status::default();
        status.set_source(Source::Epics);

        sync.alarms_states
            .write()
            .await
            .insert(String::new(), status.clone().into());

        status.set_state(State::Acknowledged);
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain("Received message for device with no matching Phoebus topic.")
            })
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_sync_valid_acknowledge_message() {
        let (sync, sender, mut receivers) = get_test_objects();

        sync.pv_metadata.write().await.insert(
            String::new(),
            PvMetadata {
                config: Config::default(),
                display_path: String::new(),
                phoebus_topic: String::from("testTopic"),
            },
        );

        let mut status = Status::default();
        status.set_source(Source::Epics);

        sync.alarms_states
            .write()
            .await
            .insert(String::new(), status.clone().into());

        status.set_state(State::Acknowledged);
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        let mut expected_command = Command::default();
        expected_command.command = ACK_COMMAND.to_string();
        expected_command.host = "Flutter Alarms App".to_string();

        let expected_key = Some(String::from("command:/"));
        let expected_value = serde_json::to_string(&expected_command).unwrap();

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async move || {
                receivers
                    .get_mut("testTopicCommand")
                    .unwrap()
                    .recv()
                    .await
                    .is_ok_and(|received| {
                        debug!("{received:?}");
                        received.key == expected_key && received.value == expected_value
                    })
            })
            .await
            .expect("Expected message was not delivered to the expected Publisher");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_sync_valid_bypass_message() {
        let (sync, sender, mut receivers) = get_test_objects();

        sync.pv_metadata.write().await.insert(
            String::new(),
            PvMetadata {
                config: Config::default(),
                display_path: String::new(),
                phoebus_topic: String::from("testTopic"),
            },
        );

        let mut status = Status::default();
        status.set_source(Source::Epics);

        sync.alarms_states
            .write()
            .await
            .insert(String::new(), status.clone().into());

        status.set_state(State::Bypassed);
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        let mut expected_config = Config::default();
        expected_config.enabled = Some(false.to_string());
        expected_config.host = "Flutter Alarms App".to_string();

        let expected_key = Some(String::from("config:/"));
        let expected_value = serde_json::to_string(&expected_config).unwrap();

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async move || {
                receivers
                    .get_mut("testTopic")
                    .unwrap()
                    .recv()
                    .await
                    .is_ok_and(|received| {
                        debug!("{received:?}");
                        received.key == expected_key && received.value == expected_value
                    })
            })
            .await
            .expect("Expected message was not delivered to the expected Publisher");
    }
}

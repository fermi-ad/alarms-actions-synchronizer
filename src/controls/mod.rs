use crate::{
    models::{
        AlarmStateCache, PvCache, Synchronizer, SynchronizerConfig,
        alarm::{
            Status,
            status::{Source, State},
        },
        phoebus::Operation,
    },
    utils::get_command_topic,
};
use rust_pubsub_lib::{Message, Publisher, Subscriber};
use std::collections::HashMap;
use tokio_stream::StreamExt;
use tracing::{debug, error, warn};

mod transform;

pub struct SyncImpl<P: Publisher, S: Subscriber> {
    alarms_states: AlarmStateCache,
    controls: S,
    phoebus_publishers: HashMap<String, P>,
    pv_metadata: PvCache,
}

impl<P: Publisher, S: Subscriber> SyncImpl<P, S> {
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

        let pv_metadata_opt = self
            .pv_metadata
            .read()
            .await
            .get(&controls_alarm.device)
            .cloned(); // Get our own copy so we can drop the reference to the shared cache
        let pv_metadata = match pv_metadata_opt {
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
                    "Received Controls alarm update for device {} with new state {:?} that does not require synchronization. Updating cache and doing nothing.",
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        models::phoebus::{Config, PvMetadata},
        utils::testing::{TestInstance, TestPublisher, TestSubscriber, get_mock_sync_config},
    };
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
    #[tracing_test::traced_test]
    async fn should_not_sync_acnet_device() {
        let (sync, sender, _) = get_test_objects();

        let mut status = Status::default();
        status.set_source(Source::Analog);

        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Received message for ACNET device"))
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_unknown_device() {
        let (sync, sender, _) = get_test_objects();

        let status = Status::default();
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || logs_contain("Received message for device '' with unknown source and no matching PV metadata."))
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_unmapped_epics_pv() {
        let (sync, sender, _) = get_test_objects();

        let mut status = Status::default();
        status.set_source(Source::Epics);
        let message = Message {
            key: None,
            value: serde_json::to_string(&status).unwrap(),
        };

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain("Received message for EPICS device '' with no matching PV metadata.")
            })
            .await
            .expect("Did not detect expected log message.");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_not_sync_when_no_cached_alarm_state() {
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

        TestInstance::check_that(sync)
            .when(sender, message)
            .satisfies(async || {
                logs_contain("No cached alarm state for device . Caching state and doing nothing.")
            })
            .await
            .expect("Did not detect expected log message.");
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
                logs_contain("Received alarm update for device  with unchanged state CachedState { state: Unknown, wake: None }. Doing nothing.")
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
}

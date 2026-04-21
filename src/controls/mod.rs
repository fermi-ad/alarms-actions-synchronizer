//! Controls Module
//!
//! Contains the [`Synchronizer`] for pushing Controls commands and configs into the Phoebus alarm server.

use crate::models::alarm::Status;
use crate::models::alarm::status::Source;
use crate::models::phoebus::{Operation, PvMetadata};
use crate::models::{
    AlarmStateCache, AttemptResult, IgnoreReason, OutOfScopeReason, PvCache, SkipReason,
    SyncDirection, SyncOutcome, Synchronizer, SynchronizerConfig,
};
use crate::utils::get_command_topic;
use rust_pubsub_lib::{Message, PubSubError, Publisher, Snapshot, StringMessage, Subscriber};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[cfg(test)]
mod tests;
mod transform;

/// Implementation of [`Synchronizer`] for pushing Controls commands and configs into the Phoebus alarm service.
pub struct SyncImpl<P: Publisher> {
    /// The atomic cache of alarm state data.
    alarms_states: AlarmStateCache,

    /// Reports when an external source has requested the end of the program.
    cancel_token: CancellationToken,

    /// The location of the Controls Kafka.
    controls_host: String,

    /// The topic to read Controls alarms from.
    controls_topic: String,

    /// The map of [`Publisher`] instances and their associated topics for use when sending updates to Phoebus.
    phoebus_publishers: HashMap<String, P>,

    /// The atomic cache of PV metadata.
    pv_metadata: PvCache,
}
impl<P: Publisher> SyncImpl<P> {
    /// Determines whether the cached state of the `controls_alarm` is current
    async fn check_cache_is_current(&self, controls_alarm: &Status) -> bool {
        self.alarms_states
            .read()
            .await
            .get(&controls_alarm.device)
            .is_some_and(|cached_state| {
                cached_state.state == controls_alarm.state()
                    && cached_state.wake == controls_alarm.wake
            })
    }

    /// Extracts the PV metadata record for the provided `device`.
    ///
    /// The presence of Phoebus metadata determines whether an EPICS device is in scope for synchronization.
    async fn get_pv_metadata(&self, device: &str) -> Option<PvMetadata> {
        self.pv_metadata.read().await.get(device).cloned() // Get our own copy so we can drop the reference to the shared cache
    }

    /// The path to follow when the operation for the `controls_alarm` is one that does not reqiure synchronization.
    /// Simply logs a debug record and updates the cache.
    async fn handle_non_sync_operation(&self, controls_alarm: &Status) -> SyncOutcome {
        debug!(
            "Received Controls alarm update for device {} with new state {:?} that does not require synchronization. Recording latest observed state for loop prevention and doing nothing.",
            controls_alarm.device,
            controls_alarm.state()
        );
        self.update_cache(controls_alarm).await;
        SyncOutcome::Ignored {
            reason: IgnoreReason::NonSyncOperation,
        }
    }

    /// Loops over elements of the [`Stream`] and processes them. Detects when a cancel has been invoked and terminates the process.
    async fn monitor(
        &self,
        mut controls_stream: impl Stream<Item = Result<StringMessage, PubSubError>> + Unpin + Send,
    ) {
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                stream_result = controls_stream.next() => {
                    match stream_result {
                        Some(item) => self.process_stream_item(item).await,
                        None => {
                            warn!("Stream from Controls closed itself. Initiating new connection.");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// The general steps for processing an EPICS device with updated state.
    async fn process_epics_message(&self, controls_alarm: &Status) -> SyncOutcome {
        let operation = transform::state_to_operation(controls_alarm.state());
        if operation == Operation::Other {
            return self.handle_non_sync_operation(controls_alarm).await;
        }

        let pv_metadata = match self.get_pv_metadata(&controls_alarm.device).await {
            Some(metadata) => metadata,
            None => {
                return handle_missing_metadata(&controls_alarm.device);
            }
        };

        let topic = match transform::get_topic_for_operation(&operation, &pv_metadata) {
            Some(topic) => topic,
            None => {
                error!(
                    "Could not find a relevant topic for operation '{operation:?}'.\n Message from Controls: {controls_alarm:?}"
                );
                self.update_cache(controls_alarm).await;
                return SyncOutcome::Skipped {
                    reason: SkipReason::MissingTopic,
                };
            }
        };

        let message = match transform::controls_to_phoebus(controls_alarm, operation, &pv_metadata)
        {
            Ok(message) => message,
            Err(err) => {
                error!(
                    "Unable to create message to send to Phoebus.\n Cause: {err}\n Message from Controls: {controls_alarm:?}"
                );
                self.update_cache(controls_alarm).await;
                return SyncOutcome::Skipped {
                    reason: SkipReason::MalformedMessage,
                };
            }
        };

        let attempt_result = match self.phoebus_publishers.get(&topic) {
            Some(publisher) => {
                if let Err(err) = publisher.publish(message).await {
                    warn!(
                        "Failed publishing to Phoebus Kafka.\n  Cause: {err}\n Message from Controls: {controls_alarm:?}"
                    );
                    AttemptResult::Failed
                } else {
                    AttemptResult::Succeeded
                }
            }
            None => {
                warn!(
                    "Received message for device with no matching Phoebus topic.\n Desired topic: {topic}\n Device: {}",
                    controls_alarm.device
                );
                self.update_cache(controls_alarm).await;
                return SyncOutcome::Skipped {
                    reason: SkipReason::MissingPublisher,
                };
            }
        };

        self.update_cache(controls_alarm).await;
        SyncOutcome::Attempted {
            direction: SyncDirection::ControlsToPhoebus,
            result: attempt_result,
        }
    }

    /// Consumes a [`Message`] and determines whether & where an update should be sent to Phoebus.
    async fn process_message(&self, msg: StringMessage) -> Result<SyncOutcome, ()> {
        let controls_alarm = deserialize_status(&msg)?;
        if self.check_cache_is_current(&controls_alarm).await {
            handle_not_stale_cached_value(controls_alarm);
            return Ok(SyncOutcome::Duplicate);
        }
        let outcome = if controls_alarm.source() == Source::Epics {
            self.process_epics_message(&controls_alarm).await
        } else {
            debug!(
                "Received ACNET device {}. Recording latest observed state for loop prevention and doing nothing.",
                controls_alarm.device
            );
            self.update_cache(&controls_alarm).await;
            SyncOutcome::Ignored {
                reason: IgnoreReason::NonEpicsSource,
            }
        };
        Ok(outcome)
    }

    /// Extracts the [`Message`] from the item retrieved from the Controls stream, or handles any errors.
    async fn process_stream_item(&self, item: Result<StringMessage, PubSubError>) {
        match item {
            Ok(msg) => {
                let _ = self.process_message(msg).await;
            }
            Err(e) => {
                warn!("Error receiving message from Controls Kafka.\n  Cause: {e:?}");
            }
        }
    }

    /// Reusable logic for updating the [`CachedState`](crate::models::CachedState) value of the `controls_alarm`.
    async fn update_cache(&self, controls_alarm: &Status) {
        self.alarms_states.write().await.insert(
            controls_alarm.device.clone(),
            controls_alarm.to_owned().into(),
        );
    }
}
#[async_trait::async_trait]
impl<P: Publisher + Send + Sync, S: Subscriber + Send + Sync> Synchronizer<P, S> for SyncImpl<P> {
    fn new(config: SynchronizerConfig) -> Self {
        SyncImpl {
            alarms_states: config.alarm_states,
            cancel_token: config.cancel_token,
            controls_host: config.controls_host,
            controls_topic: config.controls_topic,
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

    async fn synchronize<SNAP: Snapshot>(self) {
        info!("Starting Controls-to-Phoebus Synchronizer");
        loop {
            let mut controls_sub = S::new(self.controls_host.clone(), self.controls_topic.clone());
            match controls_sub.get_stream().await {
                Ok(controls_stream) => self.monitor(controls_stream).await,
                Err(e) => error!("Failed to get Controls alarms stream: {e}"),
            }

            if self.cancel_token.is_cancelled() {
                return;
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

/// The logic for transforming the value of the provided [`Message`] into a [`Status`].
fn deserialize_status(msg: &StringMessage) -> Result<Status, ()> {
    serde_json::from_str::<Status>(&msg.value()).map_err(|e| {
        error!(
            "Failed to deserialize Controls message value: {e}\n Message value: {}",
            msg.value()
        )
    })
}

/// Logs a warning that the provided EPICS device does not have any cached PV metadata, so could not be synced to Phoebus.
fn handle_missing_metadata(device: &str) -> SyncOutcome {
    warn!(
        "Received message for EPICS device '{device}' with no matching PV metadata. Treating device as out of scope until Phoebus configuration metadata is discovered. Message will be dropped."
    );
    SyncOutcome::OutOfScope {
        reason: OutOfScopeReason::MissingPhoebusMetadata,
    }
}

/// Logs a message that the `controls_alarm` state is already up to date, so no action will be taken.
fn handle_not_stale_cached_value(controls_alarm: Status) {
    debug!(
        "Received alarm update for device {} with unchanged state '{:?}'. Treating message as a duplicate of the latest observed state and doing nothing.",
        controls_alarm.device,
        controls_alarm.state()
    );
}

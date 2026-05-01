//! Controls Module
//!
//! Contains the [`Synchronizer`] for pushing Controls commands and configs into the Phoebus alarm server.
//!
//! This side of the service listens to Controls Kafka, filters for synchronization-relevant EPICS alarm actions,
//! and mirrors only the supported Phoebus-facing user intent.
//!
//! Important asymmetries and policy notes:
//! - only EPICS devices with Phoebus configuration metadata in [`PvCache`](src/models/mod.rs) are in scope
//! - ACNET updates and other non-sync Controls state are recorded only as latest observed local state for loop prevention
//! - the shared [`AlarmStateCache`](src/models/mod.rs) stores latest observed in-scope state, not latest confirmed mirroring success
//! - missing Phoebus metadata means "out of scope right now," including for devices that may appear later through runtime Phoebus discovery

use crate::models::alarm::Status;
use crate::models::alarm::status::Source;
use crate::models::metadata::MetadataScope;
use crate::models::phoebus::{Operation, PvMetadata};
use crate::models::{
    AlarmStateCache, ControlsObservedStatePolicy, IgnoreReason, OutOfScopeReason,
    OutboundSyncResult, RuntimeSyncFactory, SyncDirection, SyncOutcome, Synchronizer,
    SynchronizerConfig, read_controls_observed_state_policy, record_controls_observed_state,
};
use crate::utils::get_command_topic;
use rust_pubsub_lib::{Message, PubSubError, Publisher, Snapshot, StringMessage, Subscriber};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

mod transform;

#[cfg(test)]
mod tests;

/// Implementation of [`Synchronizer`] for pushing Controls commands and configs into the Phoebus alarm service.
pub struct SyncImpl<P: Publisher> {
    /// The atomic cache of alarm state data.
    alarm_states: AlarmStateCache,

    /// Reports when an external source has requested the end of the program.
    cancel_token: CancellationToken,

    /// The location of the Controls Kafka.
    controls_host: String,

    /// The topic to read Controls alarms from.
    controls_topic: String,

    /// The map of [`Publisher`] instances and their associated topics for use when sending updates to Phoebus.
    phoebus_publishers: HashMap<String, P>,

    /// The metadata and scope abstraction for PV discovery and lookup.
    metadata_scope: MetadataScope,
}

impl<P: Publisher> SyncImpl<P> {
    /// Creates the shared observed-state policy for an incoming Controls alarm.
    async fn observed_state_policy(&self, controls_alarm: &Status) -> ControlsObservedStatePolicy {
        read_controls_observed_state_policy(&self.alarm_states, controls_alarm).await
    }

    /// Extracts the PV metadata record for the provided `device`.
    ///
    /// The presence of Phoebus metadata determines whether an EPICS device is in scope for synchronization.
    async fn get_pv_metadata(&self, device: &str) -> Option<PvMetadata> {
        self.metadata_scope.lookup_metadata_by_device(device).await
    }

    /// Records a Controls update that carries no synchronization-relevant user intent for Phoebus.
    async fn record_non_sync_controls_state(&self, controls_alarm: &Status) -> SyncOutcome {
        debug!(
            "Received Controls alarm update for device {} with new state {:?} that does not require synchronization. Recording latest observed state for loop prevention and doing nothing.",
            controls_alarm.device,
            controls_alarm.state()
        );
        record_controls_observed_state(
            &self.alarm_states,
            &self.observed_state_policy(controls_alarm).await,
        )
        .await;
        SyncOutcome::Ignored {
            reason: IgnoreReason::UnsupportedOperation,
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

    /// Returns the structured result of attempting to mirror a Controls alarm into Phoebus.
    async fn sync_epics_message_to_phoebus(
        &self,
        controls_alarm: &Status,
        operation: Operation,
        pv_metadata: &PvMetadata,
    ) -> OutboundSyncResult {
        let topic = match transform::get_topic_for_operation(&operation, pv_metadata) {
            Some(topic) => topic,
            None => {
                error!(
                    "Could not find a relevant topic for operation '{operation:?}'.\n Message from Controls: {controls_alarm:?}"
                );
                return OutboundSyncResult::Skipped {
                    reason: crate::models::SkipReason::MissingTopic,
                };
            }
        };

        let message = match transform::controls_to_phoebus(controls_alarm, operation, pv_metadata) {
            Ok(message) => message,
            Err(err) => {
                error!(
                    "Unable to create message to send to Phoebus.\n Cause: {err}\n Message from Controls: {controls_alarm:?}"
                );
                return OutboundSyncResult::Skipped {
                    reason: crate::models::SkipReason::MalformedMessage,
                };
            }
        };

        match self.phoebus_publishers.get(&topic) {
            Some(publisher) => {
                if let Err(err) = publisher.publish(message).await {
                    warn!(
                        "Failed publishing to Phoebus Kafka.\n  Cause: {err}\n Message from Controls: {controls_alarm:?}"
                    );
                    OutboundSyncResult::Failed
                } else {
                    OutboundSyncResult::Succeeded
                }
            }
            None => {
                warn!(
                    "Received message for device with no matching Phoebus topic.\n Desired topic: {topic}\n Device: {}",
                    controls_alarm.device
                );
                OutboundSyncResult::Skipped {
                    reason: crate::models::SkipReason::MissingPublisher,
                }
            }
        }
    }

    /// The general steps for processing an EPICS device with updated state.
    async fn process_epics_message(&self, controls_alarm: &Status) -> SyncOutcome {
        match decide_epics_sync(
            controls_alarm,
            self.get_pv_metadata(&controls_alarm.device).await,
        ) {
            ControlsInboundDecision::IgnoreNonSyncState => {
                self.record_non_sync_controls_state(controls_alarm).await
            }
            ControlsInboundDecision::OutOfScope { reason } => {
                handle_out_of_scope_decision(&controls_alarm.device, reason)
            }
            ControlsInboundDecision::SyncToPhoebus {
                operation,
                pv_metadata,
            } => {
                let outbound_result = self
                    .sync_epics_message_to_phoebus(controls_alarm, operation, &pv_metadata)
                    .await;

                debug!(
                    "Refreshing latest observed Controls state for device {} after outbound result {:?} to preserve duplicate suppression and loop prevention.",
                    controls_alarm.device, outbound_result
                );
                record_controls_observed_state(
                    &self.alarm_states,
                    &self.observed_state_policy(controls_alarm).await,
                )
                .await;

                outbound_result.into_sync_outcome(SyncDirection::ControlsToPhoebus)
            }
        }
    }

    /// Consumes a [`Message`] and determines whether it carries synchronization-relevant intent for Phoebus.
    async fn process_runtime_message(&self, msg: StringMessage) -> Result<SyncOutcome, ()> {
        let controls_alarm = deserialize_status(&msg)?;
        let observed_state_policy = self.observed_state_policy(&controls_alarm).await;
        if observed_state_policy.suppresses_duplicate() {
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
            record_controls_observed_state(&self.alarm_states, &observed_state_policy).await;
            SyncOutcome::Ignored {
                reason: IgnoreReason::ExternalSource,
            }
        };
        Ok(outcome)
    }

    /// Extracts the [`Message`] from the item retrieved from the Controls stream, or handles any errors.
    async fn process_stream_item(&self, item: Result<StringMessage, PubSubError>) {
        match item {
            Ok(msg) => {
                let _ = self.process_runtime_message(msg).await;
            }
            Err(e) => {
                warn!("Error receiving message from Controls Kafka.\n  Cause: {e:?}");
            }
        }
    }
}

#[async_trait::async_trait]
impl<P: Publisher + Send + Sync, S: Subscriber + Send + Sync> Synchronizer<P, S> for SyncImpl<P> {
    fn new(config: SynchronizerConfig) -> Self {
        SyncImpl {
            alarm_states: config.alarm_states,
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
            metadata_scope: config.metadata_scope,
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

#[async_trait::async_trait]
impl RuntimeSyncFactory for SyncImpl<rust_pubsub_lib::KafkaPublisher> {
    fn new(config: SynchronizerConfig) -> Self {
        <Self as Synchronizer<rust_pubsub_lib::KafkaPublisher, rust_pubsub_lib::KafkaSubscriber>>::new(config)
    }

    async fn run(self) {
        <Self as Synchronizer<rust_pubsub_lib::KafkaPublisher, rust_pubsub_lib::KafkaSubscriber>>::synchronize::<rust_pubsub_lib::KafkaSnapshot>(self).await
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

/// Structured decision for how Controls inbound EPICS state should be handled before side effects occur.
#[derive(Debug)]
enum ControlsInboundDecision {
    IgnoreNonSyncState,
    OutOfScope {
        reason: OutOfScopeReason,
    },
    SyncToPhoebus {
        operation: Operation,
        pv_metadata: PvMetadata,
    },
}

/// Decides how an inbound Controls EPICS alarm should be handled before transport or cache side effects occur.
fn decide_epics_sync(
    controls_alarm: &Status,
    pv_metadata: Option<PvMetadata>,
) -> ControlsInboundDecision {
    let sync_action = transform::state_to_sync_action(controls_alarm.state());
    let operation = match sync_action.to_operation() {
        Some(operation) => operation,
        None => return ControlsInboundDecision::IgnoreNonSyncState,
    };

    let pv_metadata = match pv_metadata {
        Some(metadata) => metadata,
        None => {
            return ControlsInboundDecision::OutOfScope {
                reason: OutOfScopeReason::MissingPhoebusMetadata,
            };
        }
    };

    ControlsInboundDecision::SyncToPhoebus {
        operation,
        pv_metadata,
    }
}

/// Logs the structured out-of-scope Controls decision and maps it to the public synchronization outcome.
fn handle_out_of_scope_decision(device: &str, reason: OutOfScopeReason) -> SyncOutcome {
    match reason {
        OutOfScopeReason::MissingPhoebusMetadata => {
            warn!(
                "Received message for EPICS device '{device}' with no matching PV metadata. Treating device as out of scope until Phoebus configuration metadata is discovered. Message will be dropped."
            );
            SyncOutcome::OutOfScope { reason }
        }
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

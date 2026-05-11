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

use std::collections::HashMap;
use std::time::Duration;

use rust_pubsub_lib::{
    KafkaPublisher, KafkaSnapshot, KafkaSubscriber, Message, PubSubError, Publisher, Snapshot,
    StringMessage, Subscriber,
};
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::models::alarm::Status;
use crate::models::alarm::status::{Source, State};
use crate::models::metadata::MetadataScope;
use crate::models::phoebus::{Operation, PvMetadata};
use crate::models::{
    AlarmStateCache, CachedState, IgnoreReason, ObservedStatePolicy, OutboundSyncResult,
    RuntimeSyncFactory, SkipReason, SyncDirection, SyncOutcome, Synchronizer, SynchronizerConfig,
    read_observed_state_policy, record_alarm_state,
};
use crate::utils::get_command_topic;

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
                    reason: SkipReason::MissingTopic,
                };
            }
        };

        let message = match transform::controls_to_phoebus(controls_alarm, operation, pv_metadata) {
            Ok(message) => message,
            Err(_) => {
                return OutboundSyncResult::Skipped {
                    reason: SkipReason::MalformedMessage,
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
                    reason: SkipReason::MissingPublisher,
                }
            }
        }
    }

    /// The general steps for processing a device with updated state.
    async fn process_controls_message(
        &self,
        controls_alarm: Status,
        previous_observed_state: ObservedStatePolicy,
    ) -> SyncOutcome {
        match decide_controls_sync(
            &controls_alarm,
            previous_observed_state,
            self.metadata_scope
                .lookup_metadata_by_device(&controls_alarm.device)
                .await,
        ) {
            ControlsInboundDecision::IgnoreNonSyncState => {
                handle_non_sync_controls_state(&controls_alarm.device, &controls_alarm.state())
            }
            ControlsInboundDecision::OutOfScope => {
                handle_out_of_scope_decision(&controls_alarm.device)
            }
            ControlsInboundDecision::SuppressedByPolicy => {
                handle_suppressed_by_policy(&controls_alarm)
            }
            ControlsInboundDecision::SyncToPhoebus {
                operation,
                pv_metadata,
                updated_state,
            } => {
                let outbound_result = self
                    .sync_epics_message_to_phoebus(&controls_alarm, operation, &pv_metadata)
                    .await;

                debug!(
                    "Refreshing latest observed Controls state for device {} after outbound result {:?} to preserve duplicate suppression and loop prevention.",
                    controls_alarm.device, outbound_result
                );
                record_alarm_state(&self.alarm_states, &controls_alarm.device, updated_state).await;

                outbound_result.into_sync_outcome(SyncDirection::ControlsToPhoebus)
            }
        }
    }

    /// Consumes a [`Message`] and determines whether it carries synchronization-relevant intent for Phoebus.
    async fn process_runtime_message(&self, msg: StringMessage) -> Result<SyncOutcome, ()> {
        let controls_alarm = deserialize_status(&msg)?;
        if controls_alarm.source() != Source::Epics {
            return Ok(handle_acnet_device(
                &controls_alarm.device,
                controls_alarm.source(),
            ));
        }
        let previous_observed_state =
            read_observed_state_policy(&self.alarm_states, &controls_alarm.device).await;
        let outcome = self
            .process_controls_message(controls_alarm, previous_observed_state)
            .await;
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
    /// Constructs a [`SyncImpl`] from the provided [`SynchronizerConfig`], initializing Phoebus publishers for each configured topic.
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

    /// Connects to the Controls Kafka and begins monitoring for alarm updates to mirror into Phoebus.
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
impl RuntimeSyncFactory for SyncImpl<KafkaPublisher> {
    /// Constructs a runtime [`SyncImpl`] using the concrete Kafka publisher and subscriber types.
    fn new(config: SynchronizerConfig) -> Self {
        <Self as Synchronizer<KafkaPublisher, KafkaSubscriber>>::new(config)
    }

    /// Runs the Controls-to-Phoebus synchronizer with the concrete Kafka runtime types.
    async fn run(self) {
        <Self as Synchronizer<KafkaPublisher, KafkaSubscriber>>::synchronize::<KafkaSnapshot>(self)
            .await
    }
}

/// Structured decision for how Controls inbound EPICS state should be handled before side effects occur.
#[derive(Debug)]
enum ControlsInboundDecision {
    /// The inbound state is not one that maps to a Phoebus synchronization action (e.g. alarmed, OK).
    IgnoreNonSyncState,
    /// The device has no Phoebus metadata, so it is not currently tracked for synchronization.
    OutOfScope,
    /// The inbound state matches or is suppressed by the latest observed state for this device.
    SuppressedByPolicy,
    /// The inbound state maps to a Phoebus operation and the device is in scope; synchronization should proceed.
    SyncToPhoebus {
        operation: Operation,
        pv_metadata: PvMetadata,
        updated_state: CachedState,
    },
}

/// The logic for transforming the value of the provided [`Message`] into a [`Status`].
fn deserialize_status(msg: &StringMessage) -> Result<Status, ()> {
    serde_json::from_str(&msg.value()).map_err(|e| {
        error!(
            "Failed to deserialize Controls message value: {e}\n Message value: {}",
            msg.value()
        )
    })
}

/// Decides how an inbound Controls EPICS alarm should be handled before transport or cache side effects occur.
fn decide_controls_sync(
    controls_alarm: &Status,
    previous_observed_state: ObservedStatePolicy,
    pv_metadata_opt: Option<PvMetadata>,
) -> ControlsInboundDecision {
    let Some(pv_metadata) = pv_metadata_opt else {
        return ControlsInboundDecision::OutOfScope;
    };

    let updated_state = CachedState::from(controls_alarm);
    let operation_opt = transform::state_to_operation(updated_state.state);
    let Some(operation) = operation_opt else {
        return ControlsInboundDecision::IgnoreNonSyncState;
    };

    if previous_observed_state.suppresses_incoming(&updated_state) {
        ControlsInboundDecision::SuppressedByPolicy
    } else {
        ControlsInboundDecision::SyncToPhoebus {
            operation,
            pv_metadata,
            updated_state,
        }
    }
}

/// Logs a Controls alarm update for an ACNET device and maps it to the public synchronization outcome.
fn handle_acnet_device(device: &str, source: Source) -> SyncOutcome {
    debug!("Received Controls alarm update for ACNET device {device} - {source:?}. Ignoring.");
    SyncOutcome::Ignored {
        reason: IgnoreReason::ExternalSource,
    }
}

/// Logs a Controls update that carries no synchronization-relevant user intent for Phoebus.
fn handle_non_sync_controls_state(device: &str, state: &State) -> SyncOutcome {
    debug!(
        "Received Controls alarm update for device {} with new state {:?} that does not require synchronization.",
        device, state
    );
    SyncOutcome::Ignored {
        reason: IgnoreReason::StateNoise,
    }
}

/// Logs the structured out-of-scope Controls decision and maps it to the public synchronization outcome.
fn handle_out_of_scope_decision(device: &str) -> SyncOutcome {
    debug!(
        "Received message for device '{device}' with no matching PV metadata. Device is not being tracked by Phoebus."
    );
    SyncOutcome::OutOfScope
}

/// Logs a Controls alarm update that was suppressed by the observed-state policy and maps it to the public synchronization outcome.
fn handle_suppressed_by_policy(controls_alarm: &Status) -> SyncOutcome {
    debug!(
        "Got stale message for device {} that is suppressed by policy. Message will be dropped.\n Inbound message:\n{:?}",
        controls_alarm.device, controls_alarm
    );
    SyncOutcome::Ignored {
        reason: IgnoreReason::SuppressedByPolicy,
    }
}

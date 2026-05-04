//! Phoebus Monitor Module
//!
//! Contains the construct that watches a given Phoebus topic and reports relevant messages to Controls.
//!
//! This runtime monitor treats Phoebus config records as both synchronization input and runtime device-discovery
//! events. That means the in-scope EPICS device set can expand after startup when new config metadata appears.
//!
//! This module is intentionally asymmetric:
//! - bypass and snooze changes from Phoebus can be mirrored into Controls through the available gRPC commands
//! - acknowledgement commands from Phoebus can be mirrored into Controls through the available gRPC commands
//! - active-alarm transitions from Phoebus back into Controls cannot yet be mirrored fully because the shared
//!   interfaces repository does not expose an upstream API for reporting that operation
//!
//! Until that upstream API exists, the active-alarm branch in this monitor is a transitional local-only path:
//! it refreshes the synchronizer's local observed cache for duplicate suppression and loop prevention, logs the
//! external blocker clearly, and intentionally skips unavailable Controls propagation.

use crate::models::alarm::status::State;
use crate::models::metadata::MetadataScope;
use crate::models::phoebus::{
    Command, Config, Key, KeyParseError, Operation, PhoebusParseError, PvMetadata,
};
use crate::models::{
    ACK_COMMAND, AlarmStateCache, CachedState, IgnoreReason, PhoebusObservedStatePolicy,
    SkipReason, SyncDirection, SyncOutcome, SynchronizerConfig, read_phoebus_observed_state_policy,
    record_phoebus_observed_state,
};
use crate::phoebus::sync::ControlsClient;
use rust_pubsub_lib::{Message, PubSubError, StringMessage, Subscriber};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[cfg(test)]
mod tests;

pub struct Monitor {
    /// The atomic cache of alarm state data.
    alarm_states: AlarmStateCache,

    /// A token to watch in case the parent process is cancelled.
    cancel_token: CancellationToken,

    /// The client for passing alarms info to the Controls alarms service.
    controls_client: ControlsClient,

    /// The location of the Phoebus alarms topics. Used during inital startup of the sync operation.
    phoebus_host: String,

    /// The topic to monitor in the Phoebus Kafka.
    topic: String,

    /// The metadata and scope abstraction for PV discovery and lookup.
    metadata_scope: MetadataScope,
}
impl Monitor {
    /// Generates a [`Monitor`] for the provided topic.
    pub fn new(
        topic: String,
        config: &SynchronizerConfig,
        controls_client: ControlsClient,
    ) -> Self {
        Monitor {
            alarm_states: Arc::clone(&config.alarm_states),
            cancel_token: config.cancel_token.clone(),
            controls_client,
            phoebus_host: config.phoebus_host.clone(),
            topic,
            metadata_scope: config.metadata_scope.clone(),
        }
    }

    /// Kicks off the asynchronous monitoring of the topic and handling of messages that appear there.
    pub async fn start<S: Subscriber>(self) {
        info!("Starting monitor for Phoebus topic: {}", self.topic);
        loop {
            let mut sub = S::new(self.phoebus_host.clone(), self.topic.clone());
            match sub.get_stream().await.as_mut() {
                Ok(phoebus_stream) => self.watch_stream(phoebus_stream).await,
                Err(e) => error!("{e}"),
            }

            if self.cancel_token.is_cancelled() {
                return;
            }

            warn!(
                "Stream for Phoebus topic {} was dropped. Attempting to reconnect.",
                self.topic
            );
            sleep(Duration::from_secs(1)).await;
        }
    }

    /// Looks up the corresponding [`PvMetadata`] for a given [`Key`], or creates one if none exists.
    ///
    /// Runtime Phoebus config messages are also the discovery path that can bring new EPICS devices into scope.
    async fn get_pv_metadata(&self, key: &Key) -> PvMetadata {
        self.metadata_scope
            .lookup_metadata_by_device(&key.device)
            .await
            .unwrap_or_else(|| {
                self.metadata_scope
                    .build_metadata_from_config(key, &Config::default(), &self.topic)
            })
    }

    /// Handles the transitional local-only path for a Phoebus config that re-activates a previously bypassed alarm.
    ///
    /// The synchronizer can observe that Phoebus considers the alarm active again, but it cannot yet mirror that
    /// transition into Controls because the shared interfaces repository does not currently expose an API for
    /// reporting active/OK alarm state back to Controls.
    async fn record_active_alarm_local_only(
        &self,
        device: &str,
        updated_state: CachedState,
    ) -> SyncOutcome {
        let policy = read_phoebus_observed_state_policy(&self.alarm_states, device).await;
        if policy.suppresses_activation_duplicate() {
            handle_already_active(device);
            return SyncOutcome::Ignored {
                reason: IgnoreReason::UnsupportedOperation,
            };
        }

        warn!(
            "Observed Phoebus config re-activate device '{}', but the synchronizer cannot mirror active/OK state back to Controls yet because the shared interfaces repository does not expose the required upstream API. Refreshing local observed cache only until that capability exists.",
            device
        );
        record_phoebus_observed_state(
            &self.alarm_states,
            device,
            &PhoebusObservedStatePolicy::for_config_record(updated_state),
        )
        .await;
        SyncOutcome::Skipped {
            reason: SkipReason::UnsupportedCapability,
        }
    }

    /// Handles when a config came in from Phoebus to bypass an active alarm.
    async fn handle_bypassed_alarm(
        &self,
        device: &str,
        updated_state: CachedState,
        user: &str,
    ) -> SyncOutcome {
        let policy = read_phoebus_observed_state_policy(&self.alarm_states, device).await;
        if policy.suppresses_bypass_duplicate(&updated_state) {
            info!(
                "Received configuration update from Phoebus to bypass alarm for device '{}', but it is already bypassed. Updating cached PV config only.",
                device
            );
            return SyncOutcome::Duplicate;
        }

        let outbound_result = match updated_state.wake {
            Some(time) => self.controls_client.snooze_alarm(device, user, time).await,
            None => self.controls_client.bypass_alarm(device, user).await,
        };

        debug!(
            "Refreshing latest observed Phoebus bypass state for device {} after outbound result {:?} to preserve duplicate suppression and loop prevention.",
            device, outbound_result
        );
        record_phoebus_observed_state(
            &self.alarm_states,
            device,
            &PhoebusObservedStatePolicy::for_config_record(updated_state),
        )
        .await;

        outbound_result.into_sync_outcome(SyncDirection::PhoebusToControls)
    }

    /// Handles a Command message coming in from Phoebus.
    async fn process_command(&self, key: Key, msg_text: String) -> SyncOutcome {
        let observed_policy =
            read_phoebus_observed_state_policy(&self.alarm_states, &key.device).await;
        let decision = match decide_phoebus_command(&msg_text, &observed_policy) {
            Ok(decision) => decision,
            Err(_) => return log_parse_error("command", &key, &msg_text),
        };

        match decision {
            PhoebusCommandDecision::IgnoreUnsupportedCommand => {
                debug!(
                    "Received Phoebus command that does not need to be processed. Doing nothing.\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
                );
                SyncOutcome::Ignored {
                    reason: IgnoreReason::UnsupportedOperation,
                }
            }
            PhoebusCommandDecision::DuplicateAcknowledgement => {
                info!(
                    "Received acknowledgement command from Phoebus for device '{}', but it is already acknowledged. Doing nothing.",
                    key.device
                );
                SyncOutcome::Duplicate
            }
            PhoebusCommandDecision::Acknowledge { user } => {
                let outbound_result = self
                    .controls_client
                    .acknowledge_alarm(&key.device, &user)
                    .await;

                debug!(
                    "Refreshing latest observed Phoebus acknowledgement state for device {} after outbound result {:?} to preserve duplicate suppression and loop prevention.",
                    key.device, outbound_result
                );
                record_phoebus_observed_state(
                    &self.alarm_states,
                    &key.device,
                    &PhoebusObservedStatePolicy::acknowledged(),
                )
                .await;

                outbound_result.into_sync_outcome(SyncDirection::PhoebusToControls)
            }
        }
    }

    /// Handles a message from Phoebus that updates the configuration of a PV.
    async fn process_config(&self, key: Key, msg_text: String) -> SyncOutcome {
        let cached_metadata = self.get_pv_metadata(&key).await;
        let decision = match decide_phoebus_config(&msg_text, &cached_metadata.config) {
            Ok(decision) => decision,
            Err(_) => return log_parse_error("config", &key, &msg_text),
        };

        let outcome = match &decision {
            PhoebusConfigDecision::DuplicateConfig { .. } => {
                info!(
                    "Received config from Phoebus for device '{}' that matches the cached config. Doing nothing.",
                    key.device
                );
                SyncOutcome::Duplicate
            }
            PhoebusConfigDecision::NoEnablementChange { .. } => SyncOutcome::Ignored {
                reason: IgnoreReason::UnsupportedOperation,
            },
            PhoebusConfigDecision::BypassOrSnooze {
                config,
                updated_state,
            } => {
                self.handle_bypassed_alarm(&key.device, updated_state.clone(), &config.user)
                    .await
            }
            PhoebusConfigDecision::RecordActiveLocally { updated_state, .. } => {
                self.record_active_alarm_local_only(&key.device, updated_state.clone())
                    .await
            }
        };

        let new_metadata = PvMetadata {
            config: decision.into_config(),
            ..cached_metadata
        };
        self.metadata_scope
            .update_cached_metadata(&key.device, new_metadata)
            .await;
        outcome
    }

    /// The primary logic for handling a new runtime message from Phoebus.
    /// Disambiguates the type of message and hands it off to the appropriate helper method.
    async fn process_runtime_message(&self, msg: StringMessage) {
        if let Some(key_str) = msg.key() {
            let value = msg.value();
            let key = match Key::parse(&key_str) {
                Ok(parsed) => parsed,
                Err(error) => {
                    let outcome = log_monitor_key_parse_outcome(&key_str, &value, &error);
                    debug!("Phoebus monitor outcome: {outcome:?}");
                    return;
                }
            };
            let outcome = match key.operation {
                Operation::Command => self.process_command(key, value).await,
                Operation::Config => self.process_config(key, value).await,
                Operation::State => process_other(key, value),
            };
            debug!("Phoebus monitor outcome: {outcome:?}");
        } else {
            error!(
                "Got message with no key. There is a problem with the pub-sub crate or with the messages in the Phoebus Kafka.\n Message: {msg:?}"
            );
        }
    }

    /// Monitors the provided [`Stream`] and processes messages that appear there. Terminates when the stream ends or a cancel is requested.
    async fn watch_stream(
        &self,
        phoebus_stream: &mut (impl Stream<Item = Result<StringMessage, PubSubError>> + Unpin + Send),
    ) {
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                stream_item = phoebus_stream.next() => {
                    match stream_item {
                        Some(stream_result) => {
                            match stream_result {
                                Ok(message) => self.process_runtime_message(message).await,
                                Err(e) => warn!("Error from within data stream: {e}"),
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    }
}

/// Structured decision for how an inbound Phoebus command should be handled before side effects occur.
#[derive(Debug, PartialEq, Eq)]
enum PhoebusCommandDecision {
    IgnoreUnsupportedCommand,
    DuplicateAcknowledgement,
    Acknowledge { user: String },
}

/// Decides how an inbound Phoebus command should be handled before transport or cache side effects occur.
fn decide_phoebus_command(
    msg_text: &str,
    observed_policy: &PhoebusObservedStatePolicy,
) -> Result<PhoebusCommandDecision, PhoebusParseError> {
    let command_msg = serde_json::from_str::<Command>(msg_text)
        .map_err(|_| PhoebusParseError::MalformedMessage)?;

    if command_msg.command != ACK_COMMAND {
        return Ok(PhoebusCommandDecision::IgnoreUnsupportedCommand);
    }

    if observed_policy.suppresses_acknowledgement_duplicate() {
        return Ok(PhoebusCommandDecision::DuplicateAcknowledgement);
    }

    Ok(PhoebusCommandDecision::Acknowledge {
        user: command_msg.user,
    })
}

/// Structured decision for how an inbound Phoebus config should be handled before side effects occur.
#[derive(Debug, PartialEq)]
enum PhoebusConfigDecision {
    DuplicateConfig {
        config: Config,
    },
    NoEnablementChange {
        config: Config,
    },
    BypassOrSnooze {
        config: Config,
        updated_state: CachedState,
    },
    RecordActiveLocally {
        config: Config,
        updated_state: CachedState,
    },
}
impl PhoebusConfigDecision {
    fn into_config(self) -> Config {
        match self {
            Self::DuplicateConfig { config }
            | Self::NoEnablementChange { config }
            | Self::BypassOrSnooze { config, .. }
            | Self::RecordActiveLocally { config, .. } => config,
        }
    }
}

/// Decides how an inbound Phoebus config should be handled before transport or cache side effects occur.
fn decide_phoebus_config(
    msg_text: &str,
    cached_config: &Config,
) -> Result<PhoebusConfigDecision, PhoebusParseError> {
    let config = serde_json::from_str::<Config>(msg_text)
        .map_err(|_| PhoebusParseError::MalformedMessage)?;

    if config == *cached_config {
        return Ok(PhoebusConfigDecision::DuplicateConfig { config });
    }

    if config.enabled == cached_config.enabled {
        return Ok(PhoebusConfigDecision::NoEnablementChange { config });
    }

    let updated_state = config.as_cached_state()?;
    match updated_state.state {
        State::Bypassed => Ok(PhoebusConfigDecision::BypassOrSnooze {
            config,
            updated_state,
        }),
        State::Ok => Ok(PhoebusConfigDecision::RecordActiveLocally {
            config,
            updated_state,
        }),
        _ => Err(PhoebusParseError::MalformedMessage),
    }
}

/// Logs the structured parse outcome for Phoebus message handling and maps it to the public synchronization outcome.
fn log_parse_error(operation: &str, key: &Key, msg_text: &str) -> SyncOutcome {
    error!(
        "Failed to deserialize Phoebus {operation}.\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
    );
    SyncOutcome::Skipped {
        reason: SkipReason::MalformedMessage,
    }
}

/// Handles when a message comes in to activate a device that was already thought to be active.
fn handle_already_active(device: &str) {
    info!(
        "Received configuration update from Phoebus to activate alarm for device '{device}', but it is already active. Updating cached config only."
    );
}

/// Handles when a message is not a command or a config.
fn process_other(key: Key, msg_text: String) -> SyncOutcome {
    debug!(
        "Received Phoebus message that is not a config or a command. Treating it as non-sync Phoebus noise and doing nothing.\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
    );
    SyncOutcome::Ignored {
        reason: IgnoreReason::PhoebusNoise,
    }
}

fn log_monitor_key_parse_outcome(key: &str, value: &str, error: &KeyParseError) -> SyncOutcome {
    match error {
        KeyParseError::UnsupportedOperation => {
            debug!(
                "Ignoring Phoebus runtime message because its key uses an unsupported operation prefix.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Ignored {
                reason: IgnoreReason::PhoebusNoise,
            }
        }
        KeyParseError::MalformedStructure => {
            warn!(
                "Skipping malformed Phoebus runtime key: expected '<operation>:<display path>/<device>'.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Skipped {
                reason: SkipReason::MalformedMessage,
            }
        }
        KeyParseError::EmptyDevice => {
            warn!(
                "Skipping Phoebus runtime key with empty device name. Empty device names are treated as invalid.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Skipped {
                reason: SkipReason::MalformedMessage,
            }
        }
    }
}

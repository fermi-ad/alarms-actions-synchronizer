//! Controls Transformations Module
//!
//! Contains various helper methods for transforming a Controls [`Status`] into an
//! appropriate [`Message`] for Phoebus.

use crate::models::ACK_COMMAND;
use crate::models::alarm::Status;
use crate::models::alarm::status::State;
use crate::models::phoebus::{Command, Config, NormalizedEnablement, Operation, PvMetadata};
use crate::utils::get_command_topic;
use chrono::{TimeZone, Utc};
use rust_pubsub_lib::{Message, StringMessage};

#[cfg(test)]
mod tests;

/// The host name to report to Phoebus when sending messages from the Sync service.
const CONTROLS_HOST: &str = "Flutter Alarms App";

/// Domain-facing description of whether a Controls state should be synchronized to Phoebus, and if so how.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncAction {
    Acknowledge,
    UpdateConfig,
    Ignore,
}

impl SyncAction {
    /// Maps a sync action to the Phoebus wire operation required to express it.
    pub fn to_operation(self) -> Option<Operation> {
        match self {
            Self::Acknowledge => Some(Operation::Command),
            Self::UpdateConfig => Some(Operation::Config),
            Self::Ignore => None,
        }
    }
}

/// Determines the domain-facing synchronization action for a given Controls [`State`].
pub fn state_to_sync_action(alarm_state: State) -> SyncAction {
    match alarm_state {
        State::Acknowledged => SyncAction::Acknowledge,
        State::Bypassed => SyncAction::UpdateConfig,
        _ => SyncAction::Ignore,
    }
}

/// Converts the provided values into a [`Message`] for Phoebus.
pub fn controls_to_phoebus(
    controls_alarm: &Status,
    operation: Operation,
    metadata: &PvMetadata,
) -> Result<StringMessage, String> {
    let transformed_key =
        get_phoebus_key(&operation, &metadata.display_path, &controls_alarm.device);
    let transformed_value = match operation {
        Operation::Command => serde_json::to_string(&Command {
            user: controls_alarm.user.clone(),
            host: CONTROLS_HOST.to_string(),
            command: ACK_COMMAND.to_string(),
        }),
        Operation::Config => serde_json::to_string(&Config {
            user: controls_alarm.user.clone(),
            host: CONTROLS_HOST.to_string(),
            enabled: normalized_enablement_from_controls(controls_alarm).as_enabled_string(),
            phoebus_specific: metadata.config.phoebus_specific.clone(),
        }),
        Operation::State => return Err(Operation::unsupported_sync_action_error()),
    }
    .map_err(|e| format!("{e:?}"))?;
    Ok(StringMessage::new(Some(transformed_key), transformed_value))
}

/// Determines the appropriate Phoebus topic based on the [`Operation`] and cached [`PvMetadata`].
pub fn get_topic_for_operation(operation: &Operation, metadata: &PvMetadata) -> Option<String> {
    match operation {
        Operation::Command => Some(get_command_topic(&metadata.phoebus_topic)),
        Operation::Config => Some(metadata.phoebus_topic.clone()),
        _ => None,
    }
}

/// Normalizes Controls state into the domain-facing Phoebus enablement concept used for outbound config messages.
fn normalized_enablement_from_controls(controls_alarm: &Status) -> NormalizedEnablement {
    if controls_alarm.state() == State::Bypassed {
        match controls_alarm
            .wake
            .and_then(|t| Utc.timestamp_opt(t.seconds, t.nanos as u32).single())
        {
            Some(dt) => NormalizedEnablement::SnoozedUntil(dt.fixed_offset()),
            None => NormalizedEnablement::Bypassed,
        }
    } else {
        NormalizedEnablement::Active
    }
}

/// Generates the [`key`](Message::key) for the Phoebus [`Message`].
fn get_phoebus_key(operation: &Operation, display_path: &String, device: &String) -> String {
    format!("{}:{}/{}", operation.get_key_prefix(), display_path, device)
}

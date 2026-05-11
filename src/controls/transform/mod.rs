//! Controls Transformations Module
//!
//! Contains various helper methods for transforming a Controls [`Status`] into an
//! appropriate [`Message`] for Phoebus.

use chrono::{TimeZone, Utc};
use rust_pubsub_lib::{Message, StringMessage};
use tracing::error;

use crate::models::ACK_COMMAND;
use crate::models::alarm::Status;
use crate::models::alarm::status::State;
use crate::models::phoebus::{Command, Config, NormalizedEnablement, Operation, PvMetadata};
use crate::utils::get_command_topic;

#[cfg(test)]
mod tests;

/// The host name to report to Phoebus when sending messages from the Sync service.
const CONTROLS_HOST: &str = "Flutter Alarms App";

/// Determines the Phoebus wire operation for a given Controls [`State`].
pub fn state_to_operation(alarm_state: State) -> Option<Operation> {
    match alarm_state {
        State::Acknowledged => Some(Operation::Command),
        State::Bypassed | State::Unbypassed => Some(Operation::Config),
        _ => None,
    }
}

/// Converts the provided values into a [`Message`] for Phoebus.
pub fn controls_to_phoebus(
    controls_alarm: &Status,
    operation: Operation,
    metadata: &PvMetadata,
) -> Result<StringMessage, ()> {
    let transformed_key =
        get_phoebus_key(&operation, &metadata.display_path, &controls_alarm.device);
    let transformed_value = match operation {
        Operation::Command => serde_json::to_string(&Command {
            user: controls_alarm.user.clone(),
            host: CONTROLS_HOST.to_string(),
            command: ACK_COMMAND.to_string(),
        }),
        Operation::Config => {
            let updated_enabled =
                normalized_enablement_from_controls(controls_alarm).as_enabled_string();
            serde_json::to_string(&Config {
                enabled: updated_enabled,
                host: CONTROLS_HOST.to_string(),
                user: controls_alarm.user.clone(),
                phoebus_specific: metadata.phoebus_config_metadata.clone(),
            })
        }
        Operation::State => {
            error!("Controls state does not map to a supported Phoebus synchronization action\n Message from Controls: {controls_alarm:?}");
            return Err(())
        },
    }
    .map_err(|e| {
                error!(
                    "Unable to create message to send to Phoebus.\n Cause: {e}\n Message from Controls: {controls_alarm:?}"
                );})?;
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
fn get_phoebus_key(operation: &Operation, display_path: &str, device: &str) -> String {
    format!("{}:{}/{}", operation.get_key_prefix(), display_path, device)
}

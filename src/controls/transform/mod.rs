//! Transform Module
//!
//! Contains various helper methods for transforming a Controls [`Status`] into an
//! appropriate [`Message`] for Phoebus.

use crate::{
    models::{
        ACK_COMMAND,
        alarm::{Status, status::State},
        phoebus::{Command, Config, Operation, PvMetadata},
    },
    utils::get_command_topic,
};
use chrono::{TimeZone, Utc};
use rust_pubsub_lib::Message;

/// The host name to report to Phoebus when sending messages from the Sync service.
const CONTROLS_HOST: &str = "Flutter Alarms App";

/// Converts the provided values into a [`Message`] for Phoebus.
pub fn controls_to_phoebus(
    controls_alarm: &Status,
    operation: Operation,
    metadata: &PvMetadata,
) -> Result<Message, String> {
    let transformed_key =
        get_phoebus_key(&operation, &metadata.display_path, &controls_alarm.device);
    let transformed_value = match operation {
        Operation::Command => serde_json::to_string(&Command {
            user: controls_alarm.user.clone(),
            host: CONTROLS_HOST.to_string(),
            command: ACK_COMMAND.to_string(),
        }),
        Operation::Config => {
            let enabled = Some(get_enabled_string(controls_alarm));
            let prev_config = metadata.config.clone();
            serde_json::to_string(&Config {
                user: controls_alarm.user.clone(),
                host: CONTROLS_HOST.to_string(),
                enabled,
                ..prev_config
            })
        }
        _ => return Err(Operation::get_err_string_for_other()),
    }
    .map_err(|e| format!("{e:?}"))?;
    Ok(Message {
        key: Some(transformed_key),
        value: transformed_value,
    })
}

/// Determines the appropriate Phoebus topic based on the [`Operation`] and cached [`PvMetadata`].
pub fn get_topic_for_operation(operation: &Operation, metadata: &PvMetadata) -> Option<String> {
    match operation {
        Operation::Command => Some(get_command_topic(&metadata.phoebus_topic)),
        Operation::Config => Some(metadata.phoebus_topic.clone()),
        _ => None,
    }
}

/// Determines the correct [`Operation`] for a given [`State`].
pub fn state_to_operation(alarm_state: State) -> Operation {
    match alarm_state {
        State::Acknowledged => Operation::Command,
        State::Bypassed => Operation::Config,
        _ => Operation::Other,
    }
}

/// Generates the [`key`](Message::key) for the Phoebus [`Message`].
fn get_phoebus_key(operation: &Operation, display_path: &String, device: &String) -> String {
    format!("{}:{}/{}", operation.get_key_prefix(), display_path, device)
}

/// Determines the [`String`] to use when populating the [`enabled`](Config::enabled) field of a Phoebus [`Config`] record.
fn get_enabled_string(controls_alarm: &Status) -> String {
    if controls_alarm.state() == State::Bypassed {
        match controls_alarm
            .wake
            .and_then(|t| Utc.timestamp_opt(t.seconds, t.nanos as u32).single())
        {
            Some(dt) => dt.to_rfc3339(),
            None => false.to_string(),
        }
    } else {
        true.to_string()
    }
}

#[cfg(test)]
mod test {
    use crate::models::generated::Timestamp;

    use super::*;

    #[test]
    fn should_convert_command() {
        let status = Status::default();
        let metadata = PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        };

        let result_message = controls_to_phoebus(&status, Operation::Command, &metadata).unwrap();
        assert_eq!(result_message.key, Some(String::from("command:/")));
        assert_eq!(
            result_message.value,
            serde_json::to_string(&Command {
                user: String::new(),
                host: CONTROLS_HOST.to_string(),
                command: ACK_COMMAND.to_string(),
            })
            .unwrap()
        );
    }

    #[test]
    fn should_convert_config() {
        let status = Status::default();
        let metadata = PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        };

        let result_message = controls_to_phoebus(&status, Operation::Config, &metadata).unwrap();
        assert_eq!(result_message.key, Some(String::from("config:/")));
        assert_eq!(
            result_message.value,
            serde_json::to_string(&Config {
                enabled: Some(true.to_string()),
                host: CONTROLS_HOST.to_string(),
                ..metadata.config
            })
            .unwrap()
        );
    }

    #[test]
    fn should_not_convert_when_not_config_or_command() {
        let status = Status::default();
        let metadata = PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        };

        let result = controls_to_phoebus(&status, Operation::Other, &metadata).unwrap_err();
        assert_eq!(result, Operation::get_err_string_for_other());
    }

    #[test]
    fn should_not_transform_when_not_config_or_command() {
        let status = Status::default();
        let metadata = PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        };

        let result = controls_to_phoebus(&status, Operation::Other, &metadata).unwrap_err();
        assert_eq!(result, Operation::get_err_string_for_other());
    }

    #[test]
    fn should_map_enabled_state() {
        let controls_alarm = Status::default();
        let result = get_enabled_string(&controls_alarm);
        assert_eq!(result, true.to_string());
    }

    #[test]
    fn should_map_bypassed_state() {
        let mut controls_alarm = Status::default();
        controls_alarm.set_state(State::Bypassed);
        let result = get_enabled_string(&controls_alarm);
        assert_eq!(result, false.to_string());
    }

    #[test]
    fn should_map_snoozed_state() {
        let mut controls_alarm = Status::default();
        controls_alarm.set_state(State::Bypassed);
        controls_alarm.wake = Some(Timestamp {
            seconds: 0,
            nanos: 0,
        });

        let result = get_enabled_string(&controls_alarm);
        assert_eq!(
            result,
            Utc.timestamp_micros(0).single().unwrap().to_rfc3339()
        );
    }
}

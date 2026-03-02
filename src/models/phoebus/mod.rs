//! Phoebus Models Module
//!
//! Contains data structures that are germane to the Phoebus environment.

use super::CachedState;
use chrono::DateTime;
use tracing::error;

/// A struct representing a message from the Command topic.
///
/// Used in the Phoebus context to acknowledge alarms.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Command {
    /// The user issuing the command.
    pub user: String,

    /// The host where the command originated.
    pub host: String,

    /// The command itself.
    pub command: String,
}

/// A struct representing a configuration message on the main Phoebus topic.
///
/// Used in the Phoebus context to enable, bypass, and snooze alarms.
///
/// A field set to [`None`] indicates `false`, or that the field should be ignored.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// The user setting the new configuration.
    pub user: String,

    /// The host the user is making the change from.
    pub host: String,

    /// The enabled state of the alarm.
    ///
    /// This is either a time or a boolean - represented as a string to handle the ambiguity. Thanks EPICS.
    pub enabled: Option<String>,

    // The remaining values are all relevant to the Phoebus environment, but will have no bearing on the operation of this application.
    // They are modeled here so that updates to the `enabled` field do not erase other configuration settings.
    pub latching: Option<bool>,
    pub annunciating: Option<bool>,
    pub delay: Option<i64>,
    pub count: Option<i64>,
    pub filter: Option<String>,
    pub guidance: Option<Vec<TitleDetails>>,
    pub displays: Option<Vec<TitleDetails>>,
    pub commands: Option<Vec<TitleDetails>>,
    pub actions: Option<Vec<TitleDetails>>,
}
impl Config {
    /// Generates an instance of [`CachedState`] based on the [`enabled`](Config::enabled) field of this [`Config`]
    pub fn as_cached_state(&self) -> CachedState {
        match self.enabled.as_ref() {
            Some(val) => match DateTime::parse_from_rfc3339(val).ok() {
                Some(dt) => CachedState::from(dt),
                None => match val.parse::<bool>().ok() {
                    Some(is_active) => CachedState::from(is_active),
                    None => {
                        error!(
                            "Could not parse the enabled state of a Phoebus config message to either a date or a bool.\n Config record: {self:?}"
                        );
                        CachedState::default()
                    }
                },
            },
            None => CachedState::bypassed(),
        }
    }
}

/// This struct is a convenience for parsing the key of a Phoebus Kafka message.
#[derive(Debug)]
pub struct Key {
    /// An [`Operation`] representing the first characters of the key string,
    /// everything before the first `:` character.
    pub operation: Operation,

    /// The middle part of the key string, describing the path to the alarm in the Phoebus display.
    pub display_path: String,

    /// The name of the PV (or 'device'); the last part of the key string. Everything after the final '/' character.
    pub device: String,
}
impl From<String> for Key {
    fn from(value: String) -> Self {
        // The device name will be everything after the final `/` character. Use reverse split to extract it more easily.
        let (prefix, device) = value.rsplit_once("/").unwrap_or((&value, ""));
        // The operation (config, command, etc.) is encoded as all the text before the first `:` character.
        let (operation_str, display_path) = prefix.split_once(":").unwrap_or((&value, ""));
        Key {
            operation: Operation::from(operation_str),
            display_path: display_path.to_owned(),
            device: device.to_owned(),
        }
    }
}

/// Encapsulates the various operations from Phoebus that this sync service will handle.
#[derive(Debug, Eq, PartialEq)]
pub enum Operation {
    Command,
    Config,
    Other,
}
impl Operation {
    /// Generates the prefix for the Kafka message key that is relevant to the current operation type.
    pub fn get_key_prefix(&self) -> &'static str {
        match self {
            Operation::Command => "command",
            Operation::Config => "config",
            Operation::Other => "",
        }
    }

    /// Provides a [`String`] to use when an attempt is made to operate on an [`Other`](Self::Other) operation.
    pub fn get_err_string_for_other() -> String {
        "Cannot operate on type 'Other'".to_string()
    }
}
impl From<&str> for Operation {
    fn from(value: &str) -> Self {
        match value {
            "command" => Operation::Command,
            "config" => Operation::Config,
            _ => Operation::Other,
        }
    }
}

/// Metadata to track about individual PV alarms.
/// Allows the sync service to push updates to Phoebus without damaging other parts of the alarm configuration.
#[derive(Clone, Debug)]
pub struct PvMetadata {
    /// The last configuration record received for this PV. Preserved so future updates to the enabled state of the alarm
    /// do not erase other config data.
    pub config: Config,

    /// The path to the PV in the Phoebus display. Extracted from the config message key.
    pub display_path: String,

    /// The topic that this PV's alarms appear in.
    pub phoebus_topic: String,
}

/// A sub-element of a Phoebus configuration record. Not relevant to this application,
/// but modeled so it is preserved when this service pushes updates to Phoebus.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TitleDetails {
    pub title: String,
    pub details: String,
    pub delay: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::models::alarm::status::State;

    #[test]
    fn should_create_key_from_string() {
        let result = Key::from("command:some/path/here/MyDevice".to_string());
        assert_eq!(result.device, "MyDevice");
        assert_eq!(result.display_path, "some/path/here");
        assert_eq!(result.operation, Operation::Command);

        let result = Key::from("config:some/other/path/here/MyDevice2".to_string());
        assert_eq!(result.device, "MyDevice2");
        assert_eq!(result.display_path, "some/other/path/here");
        assert_eq!(result.operation, Operation::Config);

        let result = Key::from("state:some/path/here/MyDevice".to_string());
        assert_eq!(result.device, "MyDevice");
        assert_eq!(result.display_path, "some/path/here");
        assert_eq!(result.operation, Operation::Other);
    }

    #[test]
    fn should_get_cached_state_from_config() {
        let mut config = Config::default();
        let mut result = config.as_cached_state();
        assert_eq!(CachedState::bypassed(), result);

        config.enabled = Some(true.to_string());
        result = config.as_cached_state();
        assert_eq!(
            CachedState {
                state: State::Ok,
                wake: None
            },
            result
        );

        config.enabled = Some("Corrupted data".to_owned());
        result = config.as_cached_state();
        assert_eq!(CachedState::default(), result);

        config.enabled = Some("2000-01-01T00:00:00.000Z".to_owned());
        result = config.as_cached_state();
        assert_eq!(
            CachedState {
                state: State::Ok,
                wake: None
            },
            result
        );
    }

    #[test]
    fn should_get_err_string_for_operation() {
        assert_eq!(
            "Cannot operate on type 'Other'",
            Operation::get_err_string_for_other()
        );
    }

    #[test]
    fn should_get_operation_key_prefix() {
        assert_eq!("command", Operation::Command.get_key_prefix());
        assert_eq!("config", Operation::Config.get_key_prefix());
        assert_eq!("", Operation::Other.get_key_prefix());
    }
}

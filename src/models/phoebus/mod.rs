//! Phoebus Models Module
//!
//! Contains data structures that are germane to the Phoebus environment.

use super::CachedState;
use chrono::DateTime;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Display;
use tracing::error;

#[cfg(test)]
mod tests;

/// A struct representing a message from the Command topic.
///
/// Used in the Phoebus context to acknowledge alarms.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Config {
    /// The enabled state of the alarm.
    ///
    /// This is either a time or a boolean - represented as a string to handle the ambiguity. Thanks EPICS.
    #[serde(default, deserialize_with = "bool_or_string")]
    pub enabled: Option<String>,

    /// The host the user is making the change from.
    pub host: String,

    /// The user setting the new configuration.
    pub user: String,

    /// The remaining values in the JSON message are all relevant to the Phoebus environment, but will have no bearing on the operation of this application.
    /// They are modeled here so that updates to the `enabled` field do not erase other configuration settings.
    #[serde(flatten)]
    pub phoebus_specific: HashMap<String, Value>,
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
    /// The name of the PV (or 'device'); the last part of the key string. Everything after the final '/' character.
    pub device: String,

    /// The middle part of the key string, describing the path to the alarm in the Phoebus display.
    pub display_path: String,

    /// An [`Operation`] representing the first characters of the key string,
    /// everything before the first `:` character.
    pub operation: Operation,
}
impl From<String> for Key {
    fn from(value: String) -> Self {
        // The device name will be everything after the final `/` character. Use reverse split to extract it more easily.
        let (prefix, device) = value.rsplit_once("/").unwrap_or((&value, ""));
        // The operation (config, command, etc.) is encoded as all the text before the first `:` character.
        let (operation_str, display_path) = prefix.split_once(":").unwrap_or((prefix, ""));
        Key {
            device: device.to_owned(),
            display_path: display_path.to_owned(),
            operation: Operation::from(operation_str),
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
    /// Provides a [`String`] to use when an attempt is made to operate on an [`Other`](Self::Other) operation.
    pub fn get_err_string_for_other() -> String {
        "Cannot operate on type 'Other'".to_string()
    }

    /// Generates the prefix for the Kafka message key that is relevant to the current operation type.
    pub fn get_key_prefix(&self) -> &'static str {
        match self {
            Operation::Command => "command",
            Operation::Config => "config",
            Operation::Other => "",
        }
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

/// This function is used by [`serde`] to convert the [`Config::enabled`] field from a JSON value to an [`Option<String>`].
/// The default [`Deserializer`] cannot handle the ambiguity of the JSON value sometimes being a
/// raw Boolean and sometimes being a string. This function resolves that ambiguity on `serde`'s behalf.
fn bool_or_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    // Need this helper enum to do the initial deserialization. This is the piece that helps serde gracefully
    // translate `true` as a `bool` and `"true"` as a `String`
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrBool {
        Str(String),
        Bool(bool),
    }
    impl Display for StringOrBool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Bool(b) => write!(f, "{b}"),
                Self::Str(s) => write!(f, "{s}"),
            }
        }
    }

    Option::<StringOrBool>::deserialize(deserializer)
        .map(|str_or_bool_opt| str_or_bool_opt.map(|str_or_bool| str_or_bool.to_string()))
}

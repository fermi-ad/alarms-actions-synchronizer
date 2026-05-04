//! Phoebus Models Module
//!
//! Contains data structures that are germane to the Phoebus environment.
//!
//! These types sit at the anti-corruption boundary between this service and Phoebus Kafka. They intentionally
//! preserve enough of the upstream wire contract to deserialize third-party JSON safely, while also providing
//! normalized domain concepts for the rest of the synchronizer to use.
//!
//! This module is also the anti-corruption boundary for the third-party Phoebus Kafka JSON contract.
//! The upstream producer is intentionally inconsistent, so the wire-facing types here are deliberately
//! more tolerant than the synchronizer's internal semantics:
//! - fields may be omitted
//! - fields may be `null`
//! - missing or null values are treated as falsy by producer convention
//! - [`Config::enabled`] may arrive as a JSON boolean, a stringified boolean, an RFC3339 timestamp,
//!   or be absent entirely

use std::collections::HashMap;
use std::fmt::Display;

use chrono::DateTime;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tracing::error;

use super::cache::CachedState;

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

/// Normalized, domain-facing interpretation of Phoebus config enablement semantics.
///
/// [`Config`] is a wire-facing tolerance type that preserves the original JSON shape closely enough to
/// deserialize third-party payloads and round-trip unrelated Phoebus-specific fields. This enum captures
/// the meaning that the rest of the synchronizer actually cares about after normalization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NormalizedEnablement {
    /// The alarm is active/enabled now.
    Active,
    /// The alarm is bypassed indefinitely.
    Bypassed,
    /// The alarm is snoozed until the provided RFC3339 time.
    SnoozedUntil(DateTime<chrono::FixedOffset>),
}
impl NormalizedEnablement {
    /// Converts normalized Phoebus enablement into the synchronizer's cached-state model.
    pub fn as_cached_state(&self) -> CachedState {
        match self {
            Self::Active => CachedState::from(true),
            Self::Bypassed => CachedState::bypassed(),
            Self::SnoozedUntil(dt) => CachedState::from(*dt),
        }
    }

    /// Encodes the normalized enablement back into the wire-format string used for [`Config::enabled`].
    pub fn as_enabled_string(&self) -> Option<String> {
        match self {
            Self::Active => Some(true.to_string()),
            Self::Bypassed => Some(false.to_string()),
            Self::SnoozedUntil(dt) => Some(dt.to_rfc3339()),
        }
    }
}

/// A struct representing a configuration message on the main Phoebus topic.
///
/// Used in the Phoebus context to enable, bypass, and snooze alarms.
///
/// This is primarily a wire-facing tolerance type for the third-party Kafka contract. Its job is to accept
/// the messy upstream JSON representations without forcing the rest of the application to reason directly
/// about them.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Config {
    /// The enabled state of the alarm as it appears on the wire.
    ///
    /// Supported incoming representations are:
    /// - an RFC3339 timestamp string meaning "snoozed until this time"
    /// - a JSON boolean such as `true` or `false`
    /// - a stringified boolean such as `"true"` or `"false"`
    /// - `null`
    /// - an omitted field
    ///
    /// Missing and null values are normalized as falsy by producer convention. This field is stored as an
    /// [`Option<String>`] because the upstream producer is inconsistent, while the synchronizer still needs
    /// to preserve the original value shape closely enough to round-trip config records.
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
    /// Normalizes the wire-facing [`Config::enabled`] field into a named domain concept.
    ///
    /// Returns [`Err(PhoebusParseError)`](PhoebusParseError) if the wire value could not be normalized into a supported enablement meaning.
    pub fn normalized_enablement(&self) -> Result<NormalizedEnablement, PhoebusParseError> {
        match self.enabled.as_deref() {
            None => Ok(NormalizedEnablement::Bypassed),
            Some(value) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
                    return Ok(NormalizedEnablement::SnoozedUntil(dt));
                }
                if let Ok(is_active) = value.parse::<bool>() {
                    return Ok(if is_active {
                        NormalizedEnablement::Active
                    } else {
                        NormalizedEnablement::Bypassed
                    });
                }
                error!(
                    "Could not normalize the enabled state of a Phoebus config message to an enablement meaning.\n Config record: {self:?}"
                );
                Err(PhoebusParseError::MalformedMessage)
            }
        }
    }

    /// Generates an instance of [`CachedState`] based on the normalized meaning of [`enabled`](Config::enabled).
    pub fn as_cached_state(&self) -> Result<CachedState, PhoebusParseError> {
        self.normalized_enablement().map(|e| e.as_cached_state())
    }
}

/// Structured reasons why parsing a Phoebus Kafka key can fail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyParseError {
    /// The key did not include the expected `<operation>:<display path>/<device>` shape.
    MalformedStructure,
    /// The operation prefix was syntactically present but is not one this synchronizer understands.
    UnsupportedOperation,
    /// The key structure resolved to an empty device name, which is treated as invalid.
    EmptyDevice,
}

/// Structured parse failure for Phoebus command or config decision-making.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhoebusParseError {
    MalformedMessage,
}

/// This struct is a convenience for parsing the key of a Phoebus Kafka message.
#[derive(Debug, PartialEq, Eq)]
pub struct Key {
    /// The name of the PV (or 'device'); the last part of the key string. Everything after the final '/' character.
    pub device: String,

    /// The middle part of the key string, describing the path to the alarm in the Phoebus display.
    pub display_path: String,

    /// An [`Operation`] representing the first characters of the key string,
    /// everything before the first `:` character.
    pub operation: Operation,
}
impl Key {
    /// Parses a Phoebus wire key into a structured representation, rejecting malformed or unsupported inputs explicitly.
    pub fn parse(value: &str) -> Result<Self, KeyParseError> {
        let (operation_str, path_with_device) = value
            .split_once(':')
            .ok_or(KeyParseError::MalformedStructure)?;
        let operation = Operation::parse(operation_str).ok_or({
            if operation_str.is_empty() {
                KeyParseError::MalformedStructure
            } else {
                KeyParseError::UnsupportedOperation
            }
        })?;
        let (display_path, device) = path_with_device
            .rsplit_once('/')
            .ok_or(KeyParseError::MalformedStructure)?;
        if device.is_empty() {
            return Err(KeyParseError::EmptyDevice);
        }

        Ok(Key {
            device: device.to_owned(),
            display_path: display_path.to_owned(),
            operation,
        })
    }
}

/// Encapsulates the Phoebus wire-level operation prefixes that this synchronizer understands.
#[derive(Debug, Eq, PartialEq)]
pub enum Operation {
    Command,
    Config,
    State,
}
impl Operation {
    /// Provides a [`String`] to use when an attempt is made to build an outbound sync message for a
    /// Controls state that does not map to a supported Phoebus synchronization action.
    pub fn unsupported_sync_action_error() -> String {
        "Controls state does not map to a supported Phoebus synchronization action".to_string()
    }

    /// Generates the prefix for the Kafka message key that is relevant to the current operation type.
    pub fn get_key_prefix(&self) -> &'static str {
        match self {
            Operation::Command => "command",
            Operation::Config => "config",
            Operation::State => "state",
        }
    }

    /// Parses the wire-level operation prefix when it is one of the supported Phoebus operation classes.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "command" => Some(Operation::Command),
            "config" => Some(Operation::Config),
            "state" => Some(Operation::State),
            _ => None,
        }
    }
}

/// Metadata to track about individual PV alarms.
/// Allows the sync service to push updates to Phoebus without damaging other parts of the alarm configuration.
#[derive(Clone, Debug, PartialEq)]
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
///
/// The upstream `enabled` field is intentionally inconsistent. Supported incoming wire representations are:
/// - raw JSON booleans like `true` and `false`
/// - strings like `"true"`, `"false"`, or RFC3339 timestamps
/// - `null`
/// - an omitted field
///
/// Missing and null values deserialize to [`None`], which the producer convention treats as falsy. The
/// normalization step in [`Config::normalized_enablement()`] then maps that wire-level value into a named
/// domain concept.
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

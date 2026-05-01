//! Phoebus Initialization Module
//!
//! Contains the logic for reading the existing values out of the Phoebus alarms topics on startup.
//!
//! This startup pass is an approximation-oriented hydration step, not a perfect reconstruction of Phoebus history.
//! It is intentionally optimized for:
//! - discovering which EPICS devices Phoebus knows about
//! - restoring config-derived bypass and snooze intent
//! - rebuilding enough local observed-state memory for duplicate suppression after restart
//!
//! It intentionally does not promise exact acknowledgement-history reconstruction because the available startup
//! Kafka evidence is incomplete for that purpose.

use crate::models::{
    AlarmStateCache, CachedState, IgnoreReason, SkipReason, SyncOutcome,
    alarm::status::State,
    metadata::MetadataScope,
    phoebus::{Config, Key, KeyParseError, Operation, PvMetadata},
    record_config_hydrated_state, record_state_hydrated_state,
};
use rust_pubsub_lib::{Message, Snapshot, StringMessage};
use serde_json::Value;
use tracing::{debug, error, info, warn};

#[cfg(test)]
mod tests;

/// Populates the [`AlarmStateCache`] and [`MetadataScope`] with the initial read of all messages in each configured Phoebus topic.
///
/// Startup hydration is intentionally approximate rather than a guaranteed reconstruction of exact historical
/// truth. Its primary purposes are:
/// - discover which devices Phoebus currently knows about and therefore which EPICS devices are in scope
/// - carry forward configuration-derived bypass/snooze intent into the local cache after restart
/// - seed duplicate-suppression memory for the next round of runtime synchronization
///
/// Startup hydration may also infer acknowledgement/alarmed/OK state from startup-only Phoebus state messages,
/// but that evidence is secondary and may be incomplete. When there is not enough reliable startup evidence to
/// reconstruct acknowledgement history exactly, the synchronizer biases toward preserving config-derived bypass
/// and snooze semantics while tolerating ambiguity in acknowledgement state.
pub async fn get_existing_messages_from_phoebus<SNAP: Snapshot>(
    phoebus_host: String,
    topics: Vec<String>,
    state_cache: &AlarmStateCache,
    metadata_scope: &MetadataScope,
) {
    for topic in topics {
        let existing_messages =
            get_existing_messages::<SNAP>(phoebus_host.clone(), topic.clone()).await;
        populate_caches(existing_messages, topic, state_cache, metadata_scope).await;
    }
}

/// Pulls in a `Vec` of all [`Message`]s on the specified topic.
async fn get_existing_messages<SNAP: Snapshot>(
    phoebus_host: String,
    topic: String,
) -> Vec<StringMessage> {
    match SNAP::get(phoebus_host, topic).await {
        Ok(messages) => messages,
        Err(e) => {
            error!("{e}");
            Vec::new()
        }
    }
}

/// Iterates through startup messages and applies only the message classes relevant to discovery and hydration.
///
/// Config records define in-scope devices and the authoritative bypass/snooze semantics. State records are used only
/// as secondary startup-only acknowledgement/alarm evidence and should not override configuration-derived bypass/snooze
/// meaning. Unrecognized operation prefixes are ignored as noise.
async fn populate_caches(
    messages: Vec<StringMessage>,
    topic: String,
    state_cache: &AlarmStateCache,
    metadata_scope: &MetadataScope,
) {
    if messages.is_empty() {
        info!("Topic {topic} has no messages");
    } else {
        info!(
            "Startup hydration loaded {} message(s) from topic '{}' before runtime monitors started. This can indicate shared-topic test contamination when tests reuse unsalted topics.",
            messages.len(),
            topic
        );
    }
    for message in messages {
        let msg_key = match message.key() {
            Some(k) => k,
            None => {
                warn!(
                    "No key provided on config/state message: {}",
                    message.value()
                );
                continue;
            }
        };
        let key = match Key::parse(&msg_key) {
            Ok(parsed) => parsed,
            Err(error) => {
                let outcome = log_startup_key_parse_outcome(&msg_key, &message.value(), &error);
                debug!("Startup hydration outcome: {outcome:?}");
                continue;
            }
        };
        let outcome = if key.operation == Operation::Config {
            handle_config(&topic, state_cache, metadata_scope, key, message.value()).await
        } else if key.operation == Operation::State {
            handle_state(state_cache, key, message.value()).await
        } else {
            debug!(
                "Ignoring Phoebus startup message with unrecognized operation prefix.\n Original message from Phoebus: {{ key: {key:?}, text: {} }}",
                message.value()
            );
            SyncOutcome::Ignored {
                reason: IgnoreReason::PhoebusNoise,
            }
        };
        debug!("Startup hydration outcome: {outcome:?}");
    }
}

/// Attempts to convert the provided `value` into an instance of [`Config`], then updates the device's
/// value in [`AlarmStateCache`] and [`PvCache`].
///
/// Config records are the discovery path for bringing devices into scope and the primary source for bypass/snooze semantics.
async fn handle_config(
    topic: &str,
    state_cache: &AlarmStateCache,
    metadata_scope: &MetadataScope,
    key: Key,
    value: String,
) -> SyncOutcome {
    let config = match serde_json::from_str::<Config>(&value) {
        Ok(c) => c,
        Err(e) => {
            warn!(
                "Failed deserializing config message: {e}\n Tried deserializing: {}",
                value
            );
            return SyncOutcome::Ignored {
                reason: IgnoreReason::PhoebusNoise,
            };
        }
    };
    let alarm_state = config.as_cached_state();
    record_config_hydrated_state(state_cache, &key.device, alarm_state).await;
    metadata_scope
        .update_cached_metadata(
            &key.device,
            PvMetadata {
                config,
                display_path: key.display_path,
                phoebus_topic: topic.to_string(),
            },
        )
        .await;
    SyncOutcome::Hydrated
}

/// Treats the provided `value` as a JSON string representing a Phoebus `state` message.
/// Attempts to read the `severity` field and map it to a startup-only acknowledgement/alarm approximation.
/// Updates [`AlarmStateCache`] with the result only when doing so would not erase stronger config-derived
/// bypass/snooze evidence.
///
/// These state messages do not define scope, and they are not used during runtime synchronization. They are only used
/// during startup hydration as secondary evidence for acknowledgement/alarmed/OK state. Because startup Kafka history
/// may be incomplete, this path intentionally tolerates acknowledgement ambiguity and prefers to preserve any existing
/// bypass/snooze state that was already derived from a config record.
async fn handle_state(state_cache: &AlarmStateCache, key: Key, value: String) -> SyncOutcome {
    let json_opt = serde_json::from_str::<Value>(&value).ok();
    match json_opt.and_then(|json| {
        json.get("severity")
            .and_then(Value::as_str)
            .map(|borrowed| borrowed.to_ascii_uppercase())
    }) {
        Some(severity) => {
            let state = if severity.ends_with("_ACK") {
                State::Acknowledged
            } else {
                match severity.as_str() {
                    "MINOR" | "MAJOR" => State::Alarmed,
                    "OK" => State::Ok,
                    _ => State::Unknown,
                }
            };

            record_state_hydrated_state(
                state_cache,
                &key.device,
                CachedState { state, wake: None },
            )
            .await;

            SyncOutcome::Hydrated
        }
        None => {
            debug!("Could not match any fields from {value}.");
            SyncOutcome::Ignored {
                reason: IgnoreReason::PhoebusNoise,
            }
        }
    }
}

fn log_startup_key_parse_outcome(key: &str, value: &str, error: &KeyParseError) -> SyncOutcome {
    match error {
        KeyParseError::UnsupportedOperation => {
            debug!(
                "Ignoring Phoebus message during startup hydration because its key uses an unsupported operation prefix.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Ignored {
                reason: IgnoreReason::PhoebusNoise,
            }
        }
        KeyParseError::MalformedStructure => {
            warn!(
                "Skipping malformed Phoebus message key during startup hydration: expected '<operation>:<display path>/<device>'.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Skipped {
                reason: SkipReason::MalformedMessage,
            }
        }
        KeyParseError::EmptyDevice => {
            warn!(
                "Skipping Phoebus message key with empty device name during startup hydration. Empty device names are treated as invalid.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Skipped {
                reason: SkipReason::MalformedMessage,
            }
        }
    }
}

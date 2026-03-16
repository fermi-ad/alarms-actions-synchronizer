//! Phoebus Initialization Module
//!
//! Contains the logic for reading the existing values out of the Phoebus alarms topics on startup.

use crate::models::{
    AlarmStateCache, CachedState, PvCache,
    alarm::status::State,
    phoebus::{Config, Key, Operation, PvMetadata},
};
use rust_pubsub_lib::{Message, Snapshot};
use serde_json::Value;
use tracing::{debug, error, info, warn};

#[cfg(test)]
mod tests;

/// Populates the [`AlarmStateCache`] and [`PvCache`] with the initial read of all messages in each configured Phoebus topic.
pub async fn get_existing_messages_from_phoebus<SNAP: Snapshot>(
    phoebus_host: String,
    topics: Vec<String>,
    state_cache: &AlarmStateCache,
    pv_cache: &PvCache,
) {
    for topic in topics {
        let existing_configs_and_states =
            get_existing_messages::<SNAP>(phoebus_host.clone(), topic.clone()).await;
        populate_caches(existing_configs_and_states, topic, state_cache, pv_cache).await;
    }
}

/// Pulls in a `Vec` of all [`Message`]s on the specified topic.
async fn get_existing_messages<SNAP: Snapshot>(
    phoebus_host: String,
    topic: String,
) -> Vec<Message> {
    match SNAP::get(phoebus_host, topic).await {
        Ok(messages) => messages,
        Err(e) => {
            error!("{e}");
            Vec::new()
        }
    }
}

/// Iterates through the [`Message`]s and determines whether they're instances of a [`Config`] record.
///
/// If so, [`handle_config`] is invoked to extract the latest configuration data for that PV.
///
/// If not, [`handle_state`] is invoked to attempt to extract the latest state. Only works if the message
/// happens to be a valid instance of Phoebus' `state` record with a `severity` field.
async fn populate_caches(
    configs_and_states: Vec<Message>,
    topic: String,
    state_cache: &AlarmStateCache,
    pv_cache: &PvCache,
) {
    if configs_and_states.is_empty() {
        info!("Topic {topic} has no messages");
    }
    for message in configs_and_states {
        let msg_key = match message.key {
            Some(k) => k,
            None => {
                warn!("No key provided on config/state message: {}", message.value);
                continue;
            }
        };
        let key = Key::from(msg_key);
        if key.operation == Operation::Config {
            handle_config(&topic, state_cache, pv_cache, key, message.value).await;
        } else {
            handle_state(state_cache, key, message.value).await;
        }
    }
}

/// Attempts to convert the provided `value` into an instance of [`Config`], then updates the device's
/// value in [`AlarmStateCache`] and [`PvCache`].
async fn handle_config(
    topic: &str,
    state_cache: &AlarmStateCache,
    pv_cache: &PvCache,
    key: Key,
    value: String,
) {
    let config = match serde_json::from_str::<Config>(&value) {
        Ok(c) => c,
        Err(e) => {
            warn!(
                "Failed deserializing config message: {e}\n Tried deserializing: {}",
                value
            );
            return;
        }
    };
    let alarm_state = config.as_cached_state();
    state_cache
        .write()
        .await
        .insert(key.device.clone(), alarm_state);
    pv_cache.write().await.insert(
        key.device,
        PvMetadata {
            config,
            display_path: key.display_path,
            phoebus_topic: topic.to_string(),
        },
    );
}

/// Treats the provided `value` as a JSON string representing a Phoebus `state` message.
/// Attempts to read the `severity` field and map it to a [`State`] value.
/// Updates [`AlarmStateCache`] with the result.
async fn handle_state(state_cache: &AlarmStateCache, key: Key, value: String) {
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

            state_cache
                .write()
                .await
                .insert(key.device.clone(), CachedState { state, wake: None });
        }
        None => debug!("Could not match any fields from {value}."),
    }
}

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

/// Populates the [`AlarmStateCache`] and [`PvCache`] with the initial read of all messages in each configured Phoebus topic.
pub async fn get_existing_messages_from_phoebus<SNAP: Snapshot>(
    phoebus_host: String,
    topics: Vec<String>,
    state_cache: &AlarmStateCache,
    pv_cache: &PvCache,
) {
    for topic in topics {
        let existing_configs_and_states =
            get_existing_messages::<SNAP>(phoebus_host.clone(), topic.clone());
        populate_caches(existing_configs_and_states, topic, state_cache, pv_cache).await;
    }
}

/// Pulls in a `Vec` of all [`Message`]s on the specified topic.
fn get_existing_messages<SNAP: Snapshot>(phoebus_host: String, topic: String) -> Vec<Message> {
    match SNAP::get(phoebus_host, topic) {
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

#[cfg(test)]
mod test {
    use super::*;
    use rust_pubsub_lib::PubSubError;
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::RwLock;

    #[derive(Debug)]
    struct ErroringSnapshot;
    impl Snapshot for ErroringSnapshot {
        fn get(_: String, _: String) -> Result<Vec<Message>, PubSubError> {
            Err(PubSubError::default())
        }
    }

    #[derive(Debug)]
    struct PopulatedSnapshot;
    impl Snapshot for PopulatedSnapshot {
        fn get(_: String, _: String) -> Result<Vec<Message>, PubSubError> {
            Ok(generate_test_messages())
        }
    }

    fn generate_test_messages() -> Vec<Message> {
        vec![
            // Tests when a message has no key
            Message {
                key: None,
                value: String::new(),
            },
            // Tests a fully malformed message
            Message {
                key: Some("not recognizable key".to_string()),
                value: "malformed text".to_string(),
            },
            // Tests an unknown severity
            Message {
                key: Some("state:/unknown_severity_device".to_string()),
                value: String::from("{ \"severity\": \"not matching\" }"),
            },
            // Tests an Ok state
            Message {
                key: Some("state:/ok_severity_device".to_string()),
                value: String::from("{ \"severity\": \"ok\" }"),
            },
            // Tests an alarmed state
            Message {
                key: Some("state:/major_severity_device".to_string()),
                value: String::from("{ \"severity\": \"major\" }"),
            },
            // Tests an alarmed state
            Message {
                key: Some("state:/minor_severity_device".to_string()),
                value: String::from("{ \"severity\": \"Minor\" }"),
            },
            // Tests an acked state
            Message {
                key: Some("state:/acked_severity_device".to_string()),
                value: String::from("{ \"severity\": \"unknown_ACK\" }"),
            },
            // Tests malformed config
            Message {
                key: Some("config:/".to_string()),
                value: String::from("not parseable"),
            },
            // Tests a bypassed config
            Message {
                key: Some("config:path/to/bypassed".to_string()),
                value: String::from("{ \"user\": \"\", \"host\": \"\", \"enabled\": \"false\" }"),
            },
        ]
    }

    #[test]
    #[tracing_test::traced_test]
    fn should_report_error_getting_snapshot() {
        let result = get_existing_messages::<ErroringSnapshot>(String::new(), String::new());
        assert!(result.is_empty());
        assert!(logs_contain(&format!("{}", PubSubError::default())));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn should_parse_existing_messages() {
        let alarm_cache = Arc::new(RwLock::new(HashMap::new()));
        let pv_cache = Arc::new(RwLock::new(HashMap::new()));

        get_existing_messages_from_phoebus::<PopulatedSnapshot>(
            String::new(),
            vec![String::new()],
            &alarm_cache,
            &pv_cache,
        )
        .await;

        assert!(logs_contain("Could not match any fields from "));
        assert!(logs_contain("Failed deserializing config message: "));
        assert!(logs_contain("No key provided on config/state message: "));

        let state_reader = alarm_cache.read().await;
        let pv_reader = pv_cache.read().await;

        assert!(
            state_reader
                .get("unknown_severity_device")
                .is_some_and(|state| state.state == State::Unknown)
        );
        assert!(
            state_reader
                .get("ok_severity_device")
                .is_some_and(|state| state.state == State::Ok)
        );
        assert!(
            state_reader
                .get("major_severity_device")
                .is_some_and(|state| state.state == State::Alarmed)
        );
        assert!(
            state_reader
                .get("minor_severity_device")
                .is_some_and(|state| state.state == State::Alarmed)
        );
        assert!(
            state_reader
                .get("acked_severity_device")
                .is_some_and(|state| state.state == State::Acknowledged)
        );

        assert!(
            state_reader
                .get("bypassed")
                .is_some_and(|state| state.state == State::Bypassed)
        );
        assert!(
            pv_reader
                .get("bypassed")
                .is_some_and(
                    |metadata| metadata.config.enabled == Some(false.to_string())
                        && metadata.display_path == "path/to"
                )
        );
    }
}

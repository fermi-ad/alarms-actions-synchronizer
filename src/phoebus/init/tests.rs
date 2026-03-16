//! Phoebus Initialization Module Tests

use super::*;
use rust_pubsub_lib::PubSubError;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Debug)]
struct ErroringSnapshot;
#[async_trait::async_trait]
impl Snapshot for ErroringSnapshot {
    async fn get(_: String, _: String) -> Result<Vec<Message>, PubSubError> {
        Err(PubSubError::default())
    }
}

#[derive(Debug)]
struct PopulatedSnapshot;
#[async_trait::async_trait]
impl Snapshot for PopulatedSnapshot {
    async fn get(_: String, _: String) -> Result<Vec<Message>, PubSubError> {
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

#[tokio::test]
#[tracing_test::traced_test]
async fn should_report_error_getting_snapshot() {
    let result = get_existing_messages::<ErroringSnapshot>(String::new(), String::new()).await;
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

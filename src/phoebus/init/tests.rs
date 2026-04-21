//! Phoebus Initialization Module Tests

use super::*;
use rust_pubsub_lib::{ByteMessage, PubSubError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct ErroringSnapshot;
#[async_trait::async_trait]
impl Snapshot for ErroringSnapshot {
    async fn get<T, M: Message<T>>(_: String, _: String) -> Result<Vec<M>, PubSubError> {
        Err(PubSubError::default())
    }
}

#[derive(Debug)]
struct PopulatedSnapshot;
#[async_trait::async_trait]
impl Snapshot for PopulatedSnapshot {
    async fn get<T, M: Message<T>>(_: String, _: String) -> Result<Vec<M>, PubSubError> {
        Ok(generate_test_messages()
            .into_iter()
            .map(|bytes| M::from_bytes(bytes.key().as_deref(), &bytes.value()))
            .collect())
    }
}

fn generate_test_messages() -> Vec<ByteMessage> {
    vec![
        // Tests when a message has no key
        ByteMessage::new(None, Vec::new()),
        // Tests a fully malformed message
        ByteMessage::new(
            Some("not recognizable key".as_bytes().to_vec()),
            "malformed text".as_bytes().to_vec(),
        ),
        // Tests an unknown severity
        ByteMessage::new(
            Some("state:/unknown_severity_device".as_bytes().to_vec()),
            "{ \"severity\": \"not matching\" }".as_bytes().to_vec(),
        ),
        // Tests an Ok state
        ByteMessage::new(
            Some("state:/ok_severity_device".as_bytes().to_vec()),
            "{ \"severity\": \"ok\" }".as_bytes().to_vec(),
        ),
        // Tests an alarmed state
        ByteMessage::new(
            Some("state:/major_severity_device".as_bytes().to_vec()),
            "{ \"severity\": \"major\" }".as_bytes().to_vec(),
        ),
        // Tests an alarmed state
        ByteMessage::new(
            Some("state:/minor_severity_device".as_bytes().to_vec()),
            "{ \"severity\": \"Minor\" }".as_bytes().to_vec(),
        ),
        // Tests an acked state
        ByteMessage::new(
            Some("state:/acked_severity_device".as_bytes().to_vec()),
            "{ \"severity\": \"unknown_ACK\" }".as_bytes().to_vec(),
        ),
        // Tests malformed config
        ByteMessage::new(
            Some("config:/".as_bytes().to_vec()),
            "not parseable".as_bytes().to_vec(),
        ),
        // Tests a bypassed config
        ByteMessage::new(
            Some("config:path/to/bypassed".as_bytes().to_vec()),
            "{ \"user\": \"\", \"host\": \"\", \"enabled\": \"false\" }"
                .as_bytes()
                .to_vec(),
        ),
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

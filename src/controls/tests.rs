//! Tests for Controls Module
//!
//! Tests the various functions in the controls module.

use super::*;
use crate::models::ACK_COMMAND;
use crate::models::alarm::status::State;
use crate::models::phoebus::{Command, Config, PvMetadata};
use crate::utils::test_runner::{
    MessageOrigin, PHOEBUS_TOPIC, TestRunner, get_mock_sync_config_salted,
};
use rust_pubsub_lib::{KafkaPublisher, KafkaSubscriber, StringMessage};
use std::sync::Arc;

type ControlsTestRunner = TestRunner<StringMessage, String, SyncImpl<KafkaPublisher>>;

async fn get_salted_test_instance() -> ControlsTestRunner {
    ControlsTestRunner::check_when(MessageOrigin::Controls, Some(get_mock_sync_config_salted()))
        .await
}

async fn get_test_instance() -> ControlsTestRunner {
    ControlsTestRunner::check_when(MessageOrigin::Controls, None).await
}

#[tokio::test]
async fn should_continue_when_no_cached_alarm_state() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    sync.pv_metadata.write().await.insert(
        String::new(),
        PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::from(PHOEBUS_TOPIC),
        },
    );

    let mut status = Status::default();
    status.set_source(Source::Epics);
    status.set_state(State::Acknowledged);
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    let mut receiver = KafkaSubscriber::new(
        test_instance.harness.host().await,
        get_command_topic(PHOEBUS_TOPIC),
    );
    let mut stream = receiver
        .get_stream::<String, StringMessage>()
        .await
        .unwrap();

    test_instance
        .has(message)
        .results_in(async || {
            stream
                .next()
                .await
                .unwrap()
                .is_ok_and(|msg| msg.key().is_some_and(|k| k == "command:/"))
        })
        .await
        .expect("Did not receive expected message");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_corrupted_controls_message() {
    let message =
        StringMessage::from_value(String::from("{ \"unknownKey\": \"Malformed message\" }"));

    get_test_instance()
        .await
        .has(message)
        .results_in(async || logs_contain("Failed to deserialize Controls message value"))
        .await
        .expect("Did not detect expected log message.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_treat_epics_device_without_phoebus_metadata_as_out_of_scope() {
    let mut status = Status::default();
    status.set_state(State::Bypassed);
    status.set_source(Source::Epics);
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    status.set_state(State::Alarmed);
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;
    sync.alarms_states
        .write()
        .await
        .insert(String::new(), status.clone().into());

    test_instance
        .has(message)
        .results_in(async || {
            logs_contain("Treating device as out of scope until Phoebus configuration metadata is discovered")
        })
        .await
        .expect("Did not detect expected log message.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_when_alarm_state_is_not_syncable() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    sync.pv_metadata.write().await.insert(
        String::new(),
        PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        },
    );

    let mut status = Status::default();
    status.set_source(Source::Epics);

    sync.alarms_states
        .write()
        .await
        .insert(String::new(), status.clone().into());

    status.set_state(State::Ok);
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    test_instance
        .has(message)
        .results_in(async || {
            logs_contain("Recording latest observed state for loop prevention and doing nothing")
        })
        .await
        .expect("Did not detect expected log message.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_treat_unchanged_state_as_duplicate() {
    let test_instance = get_salted_test_instance().await;
    let sync = &test_instance.sync;

    sync.pv_metadata.write().await.insert(
        String::new(),
        PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        },
    );

    let mut status = Status::default();
    status.set_source(Source::Epics);
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    sync.alarms_states
        .write()
        .await
        .insert(String::new(), status.into());

    test_instance
        .has(message)
        .results_in(async || {
            logs_contain(
                "Treating message as a duplicate of the latest observed state and doing nothing.",
            )
        })
        .await
        .expect("Did not detect expected log message.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_when_no_publisher_for_topic() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    sync.pv_metadata.write().await.insert(
        String::new(),
        PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        },
    );

    let mut status = Status::default();
    status.set_source(Source::Epics);

    sync.alarms_states
        .write()
        .await
        .insert(String::new(), status.clone().into());

    status.set_state(State::Acknowledged);
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    test_instance
        .has(message)
        .results_in(async || {
            logs_contain("Received message for device with no matching Phoebus topic.")
        })
        .await
        .expect("Did not detect expected log message.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_record_acnet_device_without_transmitting() {
    let mut status = Status::default();
    status.set_source(Source::Analog);

    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    get_salted_test_instance()
        .await
        .has(message)
        .results_in(async || {
            logs_contain("Recording latest observed state for loop prevention and doing nothing")
        })
        .await
        .expect("Did not detect expected log message.");
}

#[tokio::test]
async fn should_not_transmit_unknown_device() {
    let status = Status::default();
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    let cache = Arc::clone(&sync.alarms_states);

    test_instance
        .has(message)
        .results_in(async || cache.read().await.contains_key(""))
        .await
        .expect("Did not detect expected log message.");
}

#[tokio::test]
async fn should_sync_valid_acknowledge_message() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    sync.pv_metadata.write().await.insert(
        String::new(),
        PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::from(PHOEBUS_TOPIC),
        },
    );

    let mut status = Status::default();
    status.set_source(Source::Epics);

    sync.alarms_states
        .write()
        .await
        .insert(String::new(), status.clone().into());

    status.set_state(State::Acknowledged);
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    let mut expected_command = Command::default();
    expected_command.command = ACK_COMMAND.to_string();
    expected_command.host = "Flutter Alarms App".to_string();

    let expected_key = Some(String::from("command:/"));
    let expected_value = serde_json::to_string(&expected_command).unwrap();

    let mut receiver = KafkaSubscriber::new(
        test_instance.harness.host().await,
        get_command_topic(PHOEBUS_TOPIC),
    );
    let mut stream = receiver.get_stream().await.unwrap();

    test_instance
        .has(message)
        .results_in(async move || {
            stream
                .next()
                .await
                .unwrap()
                .is_ok_and(|received: StringMessage| {
                    received.key() == expected_key && received.value() == expected_value
                })
        })
        .await
        .expect("Expected message was not delivered to the expected Publisher");
}

#[tokio::test]
async fn should_sync_valid_bypass_message() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    sync.pv_metadata.write().await.insert(
        String::new(),
        PvMetadata {
            config: Config::default(),
            display_path: String::new(),
            phoebus_topic: String::from(PHOEBUS_TOPIC),
        },
    );

    let mut status = Status::default();
    status.set_source(Source::Epics);

    sync.alarms_states
        .write()
        .await
        .insert(String::new(), status.clone().into());

    status.set_state(State::Bypassed);
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    let mut expected_config = Config::default();
    expected_config.enabled = Some(false.to_string());
    expected_config.host = "Flutter Alarms App".to_string();

    let expected_key = Some(String::from("config:/"));
    let expected_value = serde_json::to_string(&expected_config).unwrap();

    let mut receiver = KafkaSubscriber::new(
        test_instance.harness.host().await,
        String::from(PHOEBUS_TOPIC),
    );
    let mut stream = receiver.get_stream().await.unwrap();

    test_instance
        .has(message)
        .results_in(async move || {
            stream
                .next()
                .await
                .unwrap()
                .is_ok_and(|received: StringMessage| {
                    received.key() == expected_key && received.value() == expected_value
                })
        })
        .await
        .expect("Expected message was not delivered to the expected Publisher");
}

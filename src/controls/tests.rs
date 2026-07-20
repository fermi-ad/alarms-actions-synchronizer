//! Tests for the Controls module.

use std::collections::HashMap;
use std::sync::Arc;

use rust_pubsub_lib::{KafkaPublisher, KafkaSubscriber, StringMessage};

use super::*;
use crate::models::outcomes::AttemptResult;
use crate::models::phoebus::{Command, Config, Operation, PvMetadata};
use crate::models::proto::common::alarm::status::State;
use crate::models::proto::google::protobuf::Timestamp;
use crate::models::{
    ACK_COMMAND, CachedState, SkipReason, SyncDirection, SyncOutcome, read_observed_state_policy,
    record_alarm_state,
};
use crate::utils::test_runner::{MessageOrigin, TestRunner};

type ControlsTestRunner = TestRunner<StringMessage, SyncImpl<KafkaPublisher>>;

async fn get_test_instance() -> ControlsTestRunner {
    ControlsTestRunner::check_when(MessageOrigin::Controls).await
}

#[tokio::test]
async fn should_continue_when_no_cached_alarm_state() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    let phoebus_topic = test_instance
        .test_config
        .phoebus_topics
        .iter()
        .find(|topic| !topic.ends_with("Command"))
        .cloned()
        .expect("Expected a base Phoebus topic publisher to exist");
    sync.metadata_scope
        .update_cached_metadata(
            "",
            PvMetadata {
                phoebus_config_metadata: HashMap::new(),
                display_path: String::new(),
                phoebus_topic: phoebus_topic.clone(),
            },
        )
        .await;

    let status = Status {
        source: Source::Epics.into(),
        state: State::Acknowledged.into(),
        ..Status::default()
    };
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    let receiver = KafkaSubscriber::new(
        test_instance.harness.host().await,
        get_command_topic(&phoebus_topic),
    );
    let mut stream = receiver.get_stream::<StringMessage>().await;

    test_instance
        .has(message)
        .results_in(async || {
            stream
                .next()
                .await
                .is_some_and(|msg| msg.key().is_some_and(|k| k == "command:/"))
        })
        .await
        .expect("Did not receive expected message");
}

#[tokio::test]
async fn should_not_sync_corrupted_controls_message() {
    let message =
        StringMessage::from_value(String::from("{ \"unknownKey\": \"Malformed message\" }"));

    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;
    let cache = Arc::clone(&sync.alarm_states);

    test_instance
        .has(message)
        .results_in(async move || cache.read().await.is_empty())
        .await
        .expect("Corrupted Controls message should not update observed alarm state cache.");
}

#[tokio::test]
async fn should_treat_epics_device_without_phoebus_metadata_as_out_of_scope() {
    let status = Status {
        state: State::Bypassed.into(),
        source: Source::Epics.into(),
        ..Status::default()
    };
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;
    let cache = Arc::clone(&sync.alarm_states);

    test_instance
        .has(message)
        .results_in(async move || cache.read().await.get("").is_none())
        .await
        .expect("Out-of-scope EPICS device should not update the observed alarm state cache.");
}

#[tokio::test]
async fn should_not_sync_when_alarm_state_is_not_syncable() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    sync.metadata_scope
        .update_cached_metadata(
            "",
            PvMetadata {
                phoebus_config_metadata: HashMap::new(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        )
        .await;

    let status = Status {
        source: Source::Epics.into(),
        state: State::Ok.into(),
        ..Status::default()
    };
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());
    let cache = Arc::clone(&sync.alarm_states);

    test_instance
        .has(message)
        .results_in(async move || cache.read().await.get("").is_none())
        .await
        .expect("Non-syncable Controls state should not update the observed alarm state cache.");
}

#[tokio::test]
async fn should_treat_unchanged_state_as_duplicate() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    sync.metadata_scope
        .update_cached_metadata(
            "",
            PvMetadata {
                phoebus_config_metadata: HashMap::new(),
                display_path: String::new(),
                phoebus_topic: String::new(),
            },
        )
        .await;

    let status = Status {
        source: Source::Epics.into(),
        ..Status::default()
    };
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    sync.alarm_states
        .write()
        .await
        .insert(String::new(), CachedState::from(&status));
    let cache = Arc::clone(&sync.alarm_states);
    let expected_cached = CachedState::from(&status);

    test_instance
        .has(message)
        .results_in(async move || {
            cache.read().await.get("").cloned() == Some(expected_cached.clone())
        })
        .await
        .expect("Duplicate Controls state should leave cached observed state unchanged.");
}

#[tokio::test]
async fn should_not_sync_when_no_publisher_for_topic() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    sync.metadata_scope
        .update_cached_metadata(
            "",
            PvMetadata {
                phoebus_config_metadata: HashMap::new(),
                display_path: String::new(),
                // Use an empty string topic — no publisher will exist for this
                phoebus_topic: String::new(),
            },
        )
        .await;

    let status = Status {
        source: Source::Epics.into(),
        ..Status::default()
    };

    let expected_cached = CachedState::from(&Status {
        state: State::Acknowledged.into(),
        ..status.clone()
    });
    let cache = Arc::clone(&sync.alarm_states);

    let message = StringMessage::from_value(
        serde_json::to_string(&Status {
            state: State::Acknowledged.into(),
            ..status
        })
        .unwrap(),
    );

    test_instance
        .has(message)
        .results_in(async move || {
            cache.read().await.get("").cloned() == Some(expected_cached.clone())
        })
        .await
        .expect(
            "Missing-publisher path should still record latest observed state for loop prevention.",
        );
}

#[tokio::test]
async fn should_ignore_acnet_device_without_transmitting() {
    let status = Status {
        source: Source::Analog.into(),
        ..Status::default()
    };

    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    let test_instance = get_test_instance().await;
    let cache = Arc::clone(&test_instance.sync.alarm_states);

    test_instance
        .has(message)
        .results_in(async move || cache.read().await.get("").is_none())
        .await
        .expect("ACNET device should be ignored without updating the observed alarm state cache.");
}

#[tokio::test]
async fn should_not_transmit_unknown_device() {
    let status = Status::default();
    let message = StringMessage::from_value(serde_json::to_string(&status).unwrap());

    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    let cache = Arc::clone(&sync.alarm_states);

    test_instance
        .has(message)
        .results_in(async || !cache.read().await.contains_key(""))
        .await
        .expect(
            "Unknown (non-EPICS) device should not be recorded in the observed alarm state cache.",
        );
}

#[test]
fn should_map_missing_metadata_decision_to_out_of_scope_outcome() {
    assert_eq!(
        handle_out_of_scope_decision("missing-device"),
        SyncOutcome::OutOfScope
    );
}

#[test]
fn should_map_missing_publisher_outbound_result_to_skipped_outcome() {
    assert_eq!(
        OutboundSyncResult::Skipped {
            reason: SkipReason::MissingPublisher,
        }
        .into_sync_outcome(SyncDirection::ControlsToPhoebus),
        SyncOutcome::Skipped {
            reason: SkipReason::MissingPublisher,
        }
    );
}

#[test]
fn should_map_failed_controls_outbound_result_to_attempted_failed_outcome() {
    assert_eq!(
        OutboundSyncResult::Failed.into_sync_outcome(SyncDirection::ControlsToPhoebus),
        SyncOutcome::Attempted {
            direction: SyncDirection::ControlsToPhoebus,
            result: AttemptResult::Failed,
        }
    );
}

#[tokio::test]
async fn should_read_controls_policy_duplicate_only_for_exact_match() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;
    let wake = Some(Timestamp {
        seconds: 42,
        nanos: 8,
    });
    let status = Status {
        device: String::from("device"),
        source: Source::Epics.into(),
        state: State::Bypassed.into(),
        wake,
        ..Status::default()
    };

    sync.alarm_states.write().await.insert(
        status.device.clone(),
        CachedState::from(&Status {
            wake: None,
            ..status.clone()
        }),
    );

    let incoming = CachedState::from(&status);
    let policy = read_observed_state_policy(&sync.alarm_states, &status.device).await;
    assert!(!policy.suppresses_incoming(&incoming));

    sync.alarm_states
        .write()
        .await
        .insert(status.device.clone(), CachedState::from(&status));

    let policy = read_observed_state_policy(&sync.alarm_states, &status.device).await;
    assert!(policy.suppresses_incoming(&incoming));
}

#[tokio::test]
async fn should_record_controls_policy_latest_incoming_state_for_local_only_paths() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;
    let status = Status {
        device: String::from("device"),
        source: Source::Analog.into(),
        state: State::Acknowledged.into(),
        ..Status::default()
    };

    sync.alarm_states.write().await.insert(
        status.device.clone(),
        CachedState::from(&Status {
            state: State::Ok.into(),
            ..status.clone()
        }),
    );

    let incoming = CachedState::from(&status);
    record_alarm_state(&sync.alarm_states, &status.device, incoming.clone()).await;

    assert_eq!(
        sync.alarm_states.read().await.get(&status.device).cloned(),
        Some(incoming)
    );
}

#[test]
fn should_decide_non_sync_epics_state_as_ignored() {
    let status = Status {
        source: Source::Epics.into(),
        state: State::Ok.into(),
        ..Status::default()
    };

    let pv_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    assert!(matches!(
        decide_controls_sync(&status, ObservedStatePolicy::new(None), Some(pv_metadata)),
        ControlsInboundDecision::IgnoreNonSyncState
    ));
}

#[test]
fn should_decide_missing_metadata_as_out_of_scope() {
    let status = Status {
        source: Source::Epics.into(),
        state: State::Bypassed.into(),
        ..Status::default()
    };

    assert!(matches!(
        decide_controls_sync(&status, ObservedStatePolicy::new(None), None),
        ControlsInboundDecision::OutOfScope
    ));
}

#[test]
fn should_decide_acknowledged_epics_state_as_command_sync() {
    let status = Status {
        source: Source::Epics.into(),
        state: State::Acknowledged.into(),
        ..Status::default()
    };
    let pv_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::from("display"),
        phoebus_topic: String::from("topic"),
    };

    match decide_controls_sync(
        &status,
        ObservedStatePolicy::new(None),
        Some(pv_metadata.clone()),
    ) {
        ControlsInboundDecision::SyncToPhoebus {
            operation,
            pv_metadata: decided_metadata,
            ..
        } => {
            assert_eq!(operation, Operation::Command);
            assert_eq!(decided_metadata.display_path, pv_metadata.display_path);
            assert_eq!(decided_metadata.phoebus_topic, pv_metadata.phoebus_topic);
            assert_eq!(
                decided_metadata.phoebus_config_metadata,
                pv_metadata.phoebus_config_metadata
            );
        }
        decision => panic!("Expected sync decision, got {decision:?}"),
    }
}

#[test]
fn should_decide_bypassed_epics_state_as_config_sync() {
    let status = Status {
        source: Source::Epics.into(),
        state: State::Bypassed.into(),
        ..Status::default()
    };
    let pv_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::from("display"),
        phoebus_topic: String::from("topic"),
    };

    match decide_controls_sync(
        &status,
        ObservedStatePolicy::new(None),
        Some(pv_metadata.clone()),
    ) {
        ControlsInboundDecision::SyncToPhoebus {
            operation,
            pv_metadata: decided_metadata,
            ..
        } => {
            assert_eq!(operation, Operation::Config);
            assert_eq!(decided_metadata.display_path, pv_metadata.display_path);
            assert_eq!(decided_metadata.phoebus_topic, pv_metadata.phoebus_topic);
            assert_eq!(
                decided_metadata.phoebus_config_metadata,
                pv_metadata.phoebus_config_metadata
            );
        }
        decision => panic!("Expected sync decision, got {decision:?}"),
    }
}

#[tokio::test]
async fn should_sync_valid_acknowledge_message() {
    let test_instance = get_test_instance().await;
    let sync = &test_instance.sync;

    let phoebus_topic = test_instance
        .test_config
        .phoebus_topics
        .iter()
        .find(|topic| !topic.ends_with("Command"))
        .cloned()
        .expect("Expected a base Phoebus topic publisher to exist");
    sync.metadata_scope
        .update_cached_metadata(
            "",
            PvMetadata {
                phoebus_config_metadata: HashMap::new(),
                display_path: String::new(),
                phoebus_topic: phoebus_topic.clone(),
            },
        )
        .await;

    let status = Status {
        source: Source::Epics.into(),
        ..Status::default()
    };

    sync.alarm_states
        .write()
        .await
        .insert(String::new(), CachedState::from(&status));

    let message = StringMessage::from_value(
        serde_json::to_string(&Status {
            state: State::Acknowledged.into(),
            ..status
        })
        .unwrap(),
    );

    let expected_command = Command {
        command: ACK_COMMAND.to_string(),
        host: "Flutter Alarms App".to_string(),
        ..Command::default()
    };

    let expected_key = Some(String::from("command:/"));
    let expected_value = serde_json::to_string(&expected_command).unwrap();

    let receiver = KafkaSubscriber::new(
        test_instance.harness.host().await,
        get_command_topic(&phoebus_topic),
    );
    let mut stream = receiver.get_stream().await;

    test_instance
        .has(message)
        .results_in(async move || {
            stream.next().await.is_some_and(|received: StringMessage| {
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

    let phoebus_topic = test_instance
        .test_config
        .phoebus_topics
        .iter()
        .find(|topic| !topic.ends_with("Command"))
        .cloned()
        .expect("Expected a base Phoebus topic publisher to exist");
    sync.metadata_scope
        .update_cached_metadata(
            "",
            PvMetadata {
                phoebus_config_metadata: HashMap::new(),
                display_path: String::new(),
                phoebus_topic: phoebus_topic.clone(),
            },
        )
        .await;

    let status = Status {
        source: Source::Epics.into(),
        ..Status::default()
    };

    sync.alarm_states
        .write()
        .await
        .insert(String::new(), CachedState::from(&status));

    let message = StringMessage::from_value(
        serde_json::to_string(&Status {
            state: State::Bypassed.into(),
            ..status
        })
        .unwrap(),
    );

    let expected_config = Config {
        enabled: Some(false.to_string()),
        host: "Flutter Alarms App".to_string(),
        ..Config::default()
    };

    let expected_key = Some(String::from("config:/"));
    let expected_value = serde_json::to_string(&expected_config).unwrap();

    let receiver = KafkaSubscriber::new(test_instance.harness.host().await, phoebus_topic);
    let mut stream = receiver.get_stream().await;

    test_instance
        .has(message)
        .results_in(async move || {
            stream.next().await.is_some_and(|received: StringMessage| {
                received.key() == expected_key && received.value() == expected_value
            })
        })
        .await
        .expect("Expected message was not delivered to the expected Publisher");
}

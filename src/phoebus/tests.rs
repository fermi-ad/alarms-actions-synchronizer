//! Tests for Phoebus Module
//!
//! Tests the various functions in the Phoebus module.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, Utc};
use rust_pubsub_lib::{Message, StringMessage};

use super::*;
use crate::models::CachedState;
use crate::models::phoebus::{Command, Config, Key, Operation, PvMetadata};
use crate::models::proto::common::alarm::status::State;
use crate::utils::test_runner::{MessageOrigin, TestRunner, send_test_message};

#[test]
fn should_map_untracked_monitor_key_to_noise_ignored_outcome() {
    assert_eq!(
        map_key_parse_error(
            "test",
            "state:display/device",
            "{}",
            &KeyParseError::UnsupportedOperation,
        ),
        SyncOutcome::Ignored {
            reason: IgnoreReason::StateNoise,
        }
    );
}

#[test]
fn should_map_malformed_monitor_key_to_skipped_outcome() {
    assert_eq!(
        map_key_parse_error(
            "test",
            "malformed-key",
            "{}",
            &KeyParseError::MalformedStructure
        ),
        SyncOutcome::Skipped {
            reason: SkipReason::MalformedMessage,
        }
    );
}

#[test]
fn should_map_empty_device_monitor_key_to_skipped_outcome() {
    assert_eq!(
        map_key_parse_error(
            "test",
            "command:display/",
            "{}",
            &KeyParseError::EmptyDevice
        ),
        SyncOutcome::Skipped {
            reason: SkipReason::MalformedMessage,
        }
    );
}

#[tokio::test]
async fn should_not_sync_corrupted_command() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);
    let message = StringMessage::new(
        Some(String::from("command:path/to/MyDevice")),
        String::from("{ \"notRealCommandMessage\": \"Should not parse\" }"),
    );

    send_test_message(
        &test_instance.harness,
        message,
        test_instance.test_config.phoebus_topics[0].clone(),
    )
    .await
    .expect("Failed to publish malformed startup command.");

    test_instance
        .has(StringMessage::new(
            Some(String::from("command:path/to/ReadinessNoop")),
            serde_json::to_string(&Command {
                host: String::from("Readiness Host"),
                command: String::from("unsupported"),
                user: String::from("readiness-user"),
            })
            .unwrap(),
        ))
        .after_init_results_in(async || alarm_states.read().await.get("MyDevice").is_none())
        .await
        .expect("Malformed startup command should not create observed alarm state.");
}

#[tokio::test]
async fn should_not_sync_corrupted_command_after_init() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);
    let message = StringMessage::new(
        Some(String::from("command:path/to/MyDevice")),
        String::from("{ \"notRealCommandMessage\": \"Should not parse\" }"),
    );
    test_instance
        .has(message)
        .after_init_results_in(async || alarm_states.read().await.get("MyDevice").is_none())
        .await
        .expect("Malformed runtime command should not create observed alarm state.");
}

#[tokio::test]
async fn should_not_sync_duplicate_acknowledgement_commands() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);

    test_instance.test_config.alarm_states.write().await.insert(
        String::from("MyDevice"),
        CachedState {
            state: State::Acknowledged,
            wake: None,
        },
    );

    let command = Command {
        host: String::from("Test Host"),
        command: String::from("acknowledge"),
        user: String::from("test-user"),
    };

    let message = StringMessage::new(
        Some(String::from("command:my/path/to/MyDevice")),
        serde_json::to_string(&command).unwrap(),
    );

    test_instance
        .has(message)
        .after_init_results_in(async || {
            alarm_states.read().await.get("MyDevice")
                == Some(&CachedState {
                    state: State::Acknowledged,
                    wake: None,
                })
        })
        .await
        .expect("Duplicate acknowledgement command should leave observed alarm state unchanged.");
}

#[tokio::test]
async fn should_sync_valid_bypass_config_with_false() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let initial_config = Config {
        enabled: Some(true.to_string()),
        ..Config::default()
    };

    sync.config
        .metadata_scope
        .update_cached_metadata(
            "MyDevice",
            PvMetadata {
                phoebus_config_metadata: initial_config.phoebus_specific,
                display_path: String::from("my/path/to"),
                phoebus_topic: sync.config.phoebus_topics[0].clone(),
            },
        )
        .await;

    let config = Config {
        enabled: Some(false.to_string()),
        ..Config::default()
    };

    let message = StringMessage::new(
        Some(String::from("config:my/path/to/MyDevice")),
        serde_json::to_string(&config).unwrap(),
    );

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Bypassed)
        })
        .await
        .expect("The alarm state was not set to 'Bypassed'");
}

#[tokio::test]
async fn should_sync_valid_bypass_config_with_none() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let initial_config = Config {
        enabled: Some(true.to_string()),
        ..Config::default()
    };

    sync.config
        .metadata_scope
        .update_cached_metadata(
            "MyDevice",
            PvMetadata {
                phoebus_config_metadata: initial_config.phoebus_specific,
                display_path: String::from("my/path/to"),
                phoebus_topic: sync.config.phoebus_topics[0].clone(),
            },
        )
        .await;

    let config = Config::default();

    let message = StringMessage::new(
        Some(String::from("config:my/path/to/MyDevice")),
        serde_json::to_string(&config).unwrap(),
    );

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Bypassed)
        })
        .await
        .expect("The alarm state was not set to 'Bypassed'");
}

#[tokio::test]
async fn should_sync_valid_snooze_config() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let initial_config = Config {
        enabled: Some(true.to_string()),
        ..Config::default()
    };

    sync.config
        .metadata_scope
        .update_cached_metadata(
            "MyDevice",
            PvMetadata {
                phoebus_config_metadata: initial_config.phoebus_specific,
                display_path: String::from("my/path/to"),
                phoebus_topic: sync.config.phoebus_topics[0].clone(),
            },
        )
        .await;

    let config = Config {
        enabled: Some((Utc::now() + Duration::hours(1)).to_rfc3339()),
        ..Config::default()
    };

    let message = StringMessage::new(
        Some(String::from("config:my/path/to/MyDevice")),
        serde_json::to_string(&config).unwrap(),
    );

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Bypassed && state.wake.is_some())
        })
        .await
        .expect("The alarm state was not set to 'Bypassed' or the state's wake value was not set");
}

#[tokio::test]
async fn should_sync_valid_active_config_with_time() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    sync.config
        .metadata_scope
        .update_cached_metadata(
            "MyDevice",
            PvMetadata {
                phoebus_config_metadata: HashMap::new(),
                display_path: String::from("my/path/to"),
                phoebus_topic: sync.config.phoebus_topics[0].clone(),
            },
        )
        .await;

    let config = Config {
        enabled: Some((Utc::now() - Duration::hours(1)).to_rfc3339()),
        ..Config::default()
    };

    let message = StringMessage::new(
        Some(String::from("config:my/path/to/MyDevice")),
        serde_json::to_string(&config).unwrap(),
    );

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Unbypassed)
        })
        .await
        .expect("The alarm state was not set to 'Unbypassed'");
}

#[tokio::test]
async fn should_not_sync_corrupted_config_on_init() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let metadata_scope = test_instance.test_config.metadata_scope.clone();
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);
    let message = StringMessage::new(
        Some(String::from("config:path/to/MyDevice")),
        String::from("{ \"notRealConfigMessage\": \"Should not parse\" }"),
    );

    send_test_message(
        &test_instance.harness,
        message,
        test_instance.test_config.phoebus_topics[0].clone(),
    )
    .await
    .expect("Failed to publish malformed startup config.");

    test_instance
        .has(StringMessage::new(
            Some(String::from("config:path/to/ReadinessNoop")),
            serde_json::to_string(&Config::default()).unwrap(),
        ))
        .after_init_results_in(async || {
            metadata_scope
                .lookup_metadata_by_device("MyDevice")
                .await
                .is_none()
                && alarm_states.read().await.get("MyDevice").is_none()
        })
        .await
        .expect(
            "Malformed startup config should not create cached metadata or observed alarm state.",
        );
}

#[tokio::test]
async fn should_not_sync_corrupted_config_after_init() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let metadata_scope = test_instance.test_config.metadata_scope.clone();
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);
    let message = StringMessage::new(
        Some(String::from("config:path/to/MyDevice")),
        String::from("{ \"notRealConfigMessage\": \"Should not parse\" }"),
    );
    test_instance
        .has(message)
        .after_init_results_in(async || {
            metadata_scope
                .lookup_metadata_by_device("MyDevice")
                .await
                .is_none()
                && alarm_states.read().await.get("MyDevice").is_none()
        })
        .await
        .expect(
            "Malformed runtime config should not create cached metadata or observed alarm state.",
        );
}

#[tokio::test]
async fn should_not_sync_duplicated_config() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let metadata_scope = test_instance.test_config.metadata_scope.clone();
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);

    let config = Config::default();

    metadata_scope
        .update_cached_metadata(
            "MyDevice",
            PvMetadata {
                phoebus_config_metadata: config.phoebus_specific.clone(),
                display_path: String::from("cached/display"),
                phoebus_topic: test_instance.test_config.phoebus_topics[0].clone(),
            },
        )
        .await;

    let expected_topic = test_instance.test_config.phoebus_topics[0].clone();
    let message = StringMessage::new(
        Some(String::from("config:path/to/MyDevice")),
        serde_json::to_string(&config).unwrap(),
    );
    test_instance
        .has(message)
        .after_init_results_in(async move || {
            metadata_scope
                .lookup_metadata_by_device("MyDevice")
                .await
                .is_some_and(|metadata| {
                    metadata.phoebus_config_metadata == config.phoebus_specific
                        && metadata.display_path == "cached/display"
                        && metadata.phoebus_topic == expected_topic
                })
                && alarm_states.read().await.get("MyDevice").is_none()
        })
        .await
        .expect(
            "Duplicate config should preserve cached metadata and avoid observed state writes.",
        );
}

#[tokio::test]
async fn should_not_sync_unexpected_enabled_states() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let metadata_scope = test_instance.test_config.metadata_scope.clone();
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);

    let initial_config = Config::default();

    test_instance
        .test_config
        .metadata_scope
        .update_cached_metadata(
            "BadEnabledStateDevice",
            PvMetadata {
                phoebus_config_metadata: initial_config.phoebus_specific.clone(),
                display_path: String::from("cached/display"),
                phoebus_topic: test_instance.test_config.phoebus_topics[0].clone(),
            },
        )
        .await;

    let expected_topic = test_instance.test_config.phoebus_topics[0].clone();
    let message = StringMessage::new(
        Some(String::from("config:path/to/BadEnabledStateDevice")),
        serde_json::to_string(&Config {
            enabled: Some(String::from("invalid value")),
            ..Config::default()
        })
        .unwrap(),
    );
    test_instance
        .has(message)
        .after_init_results_in(async move || {
            metadata_scope
                .lookup_metadata_by_device("BadEnabledStateDevice")
                .await
                .is_some_and(|metadata| {
                    metadata.phoebus_config_metadata == initial_config.phoebus_specific
                        && metadata.display_path == "cached/display"
                        && metadata.phoebus_topic == expected_topic
                })
                && alarm_states
                    .read()
                    .await
                    .get("BadEnabledStateDevice")
                    .is_none()
        })
        .await
        .expect(
            "Malformed enablement config should not overwrite cached metadata or observed state.",
        );
}

#[tokio::test]
async fn should_sync_active_config() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let sync = &test_instance.sync;

    let metadata_scope = sync.config.metadata_scope.clone();
    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let previous_config = Config {
        enabled: Some(false.to_string()),
        ..Config::default()
    };

    sync.config
        .metadata_scope
        .update_cached_metadata(
            "MyDevice",
            PvMetadata {
                phoebus_config_metadata: previous_config.phoebus_specific,
                display_path: String::from("cached/display"),
                phoebus_topic: sync.config.phoebus_topics[0].clone(),
            },
        )
        .await;
    sync.config.alarm_states.write().await.insert(
        String::from("MyDevice"),
        CachedState {
            state: State::Bypassed,
            wake: None,
        },
    );

    let active_config = Config {
        enabled: Some(true.to_string()),
        user: String::from("runtime user"),
        ..Config::default()
    };

    let message = StringMessage::new(
        Some(String::from("config:path/to/MyDevice")),
        serde_json::to_string(&active_config).unwrap(),
    );
    let expected_topic = test_instance.test_config.phoebus_topics[0].clone();
    test_instance
        .has(message)
        .after_init_results_in(async move || {
            metadata_scope
                .lookup_metadata_by_device("MyDevice")
                .await
                .is_some_and(|metadata| {
                    metadata.phoebus_config_metadata == active_config.phoebus_specific
                        && metadata.display_path == "cached/display"
                        && metadata.phoebus_topic == expected_topic
                })
                && alarm_states.read().await.get("MyDevice")
                    == Some(&CachedState {
                        state: State::Unbypassed,
                        wake: None,
                    })
        })
        .await
        .expect("Active config should refresh cached config and observed state.");
}

#[tokio::test]
async fn should_not_sync_already_bypassed_config() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let metadata_scope = test_instance.test_config.metadata_scope.clone();
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);

    let initial_config = Config {
        enabled: Some(true.to_string()),
        ..Config::default()
    };

    test_instance
        .test_config
        .metadata_scope
        .update_cached_metadata(
            "AlreadyBypassedDevice",
            PvMetadata {
                phoebus_config_metadata: initial_config.phoebus_specific,
                display_path: String::from("cached/display"),
                phoebus_topic: test_instance.test_config.phoebus_topics[0].clone(),
            },
        )
        .await;
    test_instance.test_config.alarm_states.write().await.insert(
        String::from("AlreadyBypassedDevice"),
        CachedState {
            state: State::Bypassed,
            wake: None,
        },
    );

    let config = Config {
        enabled: Some(false.to_string()),
        ..Config::default()
    };

    let message = StringMessage::new(
        Some(String::from("config:cached/display/AlreadyBypassedDevice")),
        serde_json::to_string(&config).unwrap(),
    );
    let expected_topic = test_instance.test_config.phoebus_topics[0].clone();
    test_instance
        .has(message)
        .after_init_results_in(async move || {
            metadata_scope.lookup_metadata_by_device("AlreadyBypassedDevice").await.is_some_and(|metadata| {
                metadata.phoebus_config_metadata == config.phoebus_specific
                    && metadata.display_path == "cached/display"
                    && metadata.phoebus_topic == expected_topic
            }) && alarm_states.read().await.get("AlreadyBypassedDevice")
                == Some(&CachedState {
                    state: State::Bypassed,
                    wake: None,
                })
        })
        .await
        .expect("Already bypassed config should refresh cached config while leaving observed alarm state bypassed.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_add_new_device_to_scope_from_runtime_config() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let sync = &test_instance.sync;

    let metadata_scope = sync.config.metadata_scope.clone();
    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let config = Config {
        enabled: Some(false.to_string()),
        ..Config::default()
    };

    let message = StringMessage::new(
        Some(String::from("config:runtime/path/NewDevice")),
        serde_json::to_string(&config).unwrap(),
    );

    test_instance
        .has(message)
        .results_in(async || {
            metadata_scope
                .lookup_metadata_by_device("NewDevice")
                .await
                .is_some_and(|metadata| {
                    // phoebus_config_metadata is a HashMap<String, Value>; for a default Config
                    // with only enabled="false", the phoebus_specific map is empty (enabled, host,
                    // user are not part of phoebus_specific).
                    metadata.phoebus_config_metadata == config.phoebus_specific
                        && metadata.display_path == "runtime/path"
                })
                && alarm_states
                    .read()
                    .await
                    .get("NewDevice")
                    .is_some_and(|state| state.state == State::Bypassed)
        })
        .await
        .expect("Runtime config did not add the new device to scope with bypassed startup state.");
}

#[tokio::test]
async fn should_not_sync_unknown_operations() {
    let test_instance =
        TestRunner::<StringMessage, SyncImpl>::check_when(MessageOrigin::Phoebus).await;
    let alarm_states = Arc::clone(&test_instance.test_config.alarm_states);

    // Pre-seed a known state so we can verify it is preserved (not overwritten) by the unknown-operation message.
    let pre_seeded = CachedState {
        state: State::Acknowledged,
        wake: None,
    };
    alarm_states
        .write()
        .await
        .insert(String::from("MyDevice"), pre_seeded.clone());

    let message = StringMessage::new(
        Some(String::from("some-other-command:path/to/MyDevice")),
        String::new(),
    );
    test_instance
        .has(message)
        .after_init_results_in(async move || {
            alarm_states.read().await.get("MyDevice").cloned() == Some(pre_seeded.clone())
        })
        .await
        .expect(
            "Unknown operation prefix should leave pre-existing observed alarm state unchanged.",
        );
}

#[test]
fn should_parse_command_key() {
    let result = Key::parse("command:path/to/MyDevice").unwrap();

    assert_eq!(result.device, "MyDevice");
    assert_eq!(result.display_path, "path/to");
    assert_eq!(result.operation, Operation::Command);
}

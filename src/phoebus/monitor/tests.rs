use chrono::{Timelike, Utc};

use super::*;
use crate::models::outcomes::AttemptResult;
use crate::models::phoebus::{Config, Key, Operation, PvMetadata};
use crate::models::proto::common::alarm::status::State;
use crate::models::proto::google::protobuf::Timestamp;
use crate::models::{
    ACK_COMMAND, CachedState, ObservedStatePolicy, OutboundSyncResult, SkipReason, SyncDirection,
    SyncOutcome,
};
use std::collections::HashMap;

#[test]
fn should_decide_malformed_phoebus_command_as_skipped_parse_error() {
    assert!(
        decide_phoebus_command(
            "{ \"notRealCommandMessage\": \"Should not parse\" }",
            &ObservedStatePolicy::new(None),
        )
        .is_err()
    );
}

#[test]
fn should_decide_non_ack_phoebus_command_as_ignored() {
    let command = Command {
        command: String::from("unsupported"),
        user: String::from("test-user"),
        ..Command::default()
    };

    assert_eq!(
        decide_phoebus_command(
            &serde_json::to_string(&command).unwrap(),
            &ObservedStatePolicy::new(None),
        ),
        Ok(PhoebusCommandDecision::IgnoreUnsupportedCommand)
    );
}

#[test]
fn should_decide_duplicate_acknowledgement_command() {
    let command = Command {
        command: ACK_COMMAND.to_string(),
        user: String::from("test-user"),
        ..Command::default()
    };

    assert_eq!(
        decide_phoebus_command(
            &serde_json::to_string(&command).unwrap(),
            &ObservedStatePolicy::new(Some(CachedState {
                state: State::Acknowledged,
                wake: None,
            })),
        ),
        Ok(PhoebusCommandDecision::SuppressedByPolicy)
    );
}

#[test]
fn should_decide_malformed_phoebus_config_as_skipped_parse_error() {
    let cached_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };
    let config = Config {
        enabled: Some(String::from("not-a-real-enabled-value")),
        ..Config::default()
    };
    assert!(
        decide_phoebus_config(&config, &cached_metadata, &ObservedStatePolicy::new(None),).is_err()
    );
}

#[test]
fn should_decide_duplicate_phoebus_config() {
    let config = Config {
        enabled: Some(String::from("false")),
        user: String::from("test-user"),
        ..Config::default()
    };

    let cached_metadata = PvMetadata {
        phoebus_config_metadata: config.phoebus_specific.clone(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    // When the observed state already matches the incoming state AND the phoebus_specific metadata
    // is identical, the decision should be DuplicateConfig.
    let observed = ObservedStatePolicy::new(Some(CachedState {
        state: State::Bypassed,
        wake: None,
    }));

    assert_eq!(
        decide_phoebus_config(&config, &cached_metadata, &observed),
        Ok(PhoebusConfigDecision::DuplicateConfig)
    );
}

#[test]
fn should_decide_config_with_same_enablement_as_metadata_only_update() {
    // The incoming config has the same enablement (both bypassed) but different phoebus_specific
    // metadata (e.g., a display label changed). This should be treated as a metadata-only update.
    let incoming_config = Config {
        enabled: Some(String::from("false")),
        user: String::from("incoming-user"),
        phoebus_specific: HashMap::from([(
            String::from("title"),
            serde_json::Value::String(String::from("new-title")),
        )]),
        ..Config::default()
    };

    // The cached metadata has different phoebus_specific (empty), so it's not a full duplicate.
    let cached_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    // The observed state already matches the incoming (both bypassed), but phoebus_specific differs.
    let observed = ObservedStatePolicy::new(Some(CachedState {
        state: State::Bypassed,
        wake: None,
    }));

    assert_eq!(
        decide_phoebus_config(&incoming_config, &cached_metadata, &observed),
        Ok(PhoebusConfigDecision::NoEnablementChange)
    );
}

#[test]
fn should_decide_bypassed_phoebus_config_as_controls_bypass() {
    let incoming_config = Config {
        enabled: Some(String::from("false")),
        user: String::from("test-user"),
        ..Config::default()
    };

    let cached_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    // The observed state is Unbypassed (active), so the incoming Bypassed state is not suppressed.
    let observed = ObservedStatePolicy::new(Some(CachedState {
        state: State::Unbypassed,
        wake: None,
    }));

    assert_eq!(
        decide_phoebus_config(&incoming_config, &cached_metadata, &observed),
        Ok(PhoebusConfigDecision::Bypass {
            updated_state: CachedState {
                state: State::Bypassed,
                wake: None,
            }
        })
    );
}

#[test]
fn should_decide_snoozed_phoebus_config_as_controls_snooze() {
    let time = Utc::now() + Duration::from_hours(24);
    let incoming_config = Config {
        enabled: Some(time.to_rfc3339()),
        user: String::from("test-user"),
        ..Config::default()
    };

    let cached_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    // The observed state is Unbypassed (active), so the incoming Snoozed state is not suppressed.
    let observed = ObservedStatePolicy::new(Some(CachedState {
        state: State::Unbypassed,
        wake: None,
    }));

    assert_eq!(
        decide_phoebus_config(&incoming_config, &cached_metadata, &observed),
        Ok(PhoebusConfigDecision::Snooze {
            updated_state: CachedState {
                state: State::Bypassed,
                wake: Some(Timestamp {
                    seconds: time.timestamp(),
                    nanos: time.nanosecond() as i32
                })
            }
        })
    );
}

#[test]
fn should_decide_active_phoebus_config_as_controls_activation() {
    let incoming_config = Config {
        enabled: Some(String::from("true")),
        user: String::from("test-user"),
        ..Config::default()
    };

    let cached_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    // The observed state is Bypassed, so the incoming Unbypassed state is not suppressed.
    let observed = ObservedStatePolicy::new(Some(CachedState {
        state: State::Bypassed,
        wake: None,
    }));

    assert_eq!(
        decide_phoebus_config(&incoming_config, &cached_metadata, &observed),
        Ok(PhoebusConfigDecision::Activate {
            updated_state: CachedState {
                state: State::Unbypassed,
                wake: None,
            }
        })
    );
}

#[test]
fn should_decide_invalid_enablement_phoebus_config_as_malformed() {
    let incoming_config = Config {
        enabled: Some(String::from("not-a-real-enabled-value")),
        user: String::from("test-user"),
        ..Config::default()
    };

    let cached_metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    let observed = ObservedStatePolicy::new(None);

    assert!(decide_phoebus_config(&incoming_config, &cached_metadata, &observed).is_err());
}

#[test]
fn should_decide_acknowledgement_command_as_controls_acknowledge() {
    let command = Command {
        command: ACK_COMMAND.to_string(),
        user: String::from("test-user"),
        ..Command::default()
    };

    let expected_updated_state = CachedState {
        state: State::Acknowledged,
        wake: None,
    };

    // When the observed state is None (no prior observation), an ack command should proceed.
    assert_eq!(
        decide_phoebus_command(
            &serde_json::to_string(&command).unwrap(),
            &ObservedStatePolicy::new(None),
        ),
        Ok(PhoebusCommandDecision::Acknowledge {
            user: String::from("test-user"),
            updated_state: expected_updated_state,
        })
    );
}

#[test]
fn should_map_malformed_phoebus_command_to_skipped_outcome() {
    let key = Key {
        device: String::from("device"),
        display_path: String::from("display"),
        operation: Operation::Command,
    };

    assert_eq!(
        log_parse_error("command", &key, "{ malformed"),
        SyncOutcome::Skipped {
            reason: SkipReason::MalformedMessage,
        }
    );
}

#[test]
fn should_map_malformed_phoebus_config_to_skipped_outcome() {
    let key = Key {
        device: String::from("device"),
        display_path: String::from("display"),
        operation: Operation::Config,
    };

    assert_eq!(
        log_parse_error("config", &key, "{ malformed"),
        SyncOutcome::Skipped {
            reason: SkipReason::MalformedMessage,
        }
    );
}

#[test]
fn should_treat_observed_acknowledged_state_as_duplicate_acknowledgement() {
    let command = Command {
        command: ACK_COMMAND.to_string(),
        user: String::from("test-user"),
        ..Command::default()
    };

    assert_eq!(
        decide_phoebus_command(
            &serde_json::to_string(&command).unwrap(),
            &ObservedStatePolicy::new(Some(CachedState {
                state: State::Acknowledged,
                wake: None,
            })),
        ),
        Ok(PhoebusCommandDecision::SuppressedByPolicy)
    );
}

#[test]
fn should_map_failed_phoebus_outbound_result_to_attempted_failed_outcome() {
    assert_eq!(
        OutboundSyncResult::Failed.into_sync_outcome(SyncDirection::PhoebusToControls),
        SyncOutcome::Attempted {
            direction: SyncDirection::PhoebusToControls,
            result: AttemptResult::Failed,
        }
    );
}

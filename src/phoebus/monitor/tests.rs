use super::*;
use crate::models::alarm::status::State;
use crate::models::cache::ObservedAlarmState;
use crate::models::outcomes::AttemptResult;
use crate::models::phoebus::{Config, Key, Operation};
use crate::models::{
    ACK_COMMAND, CachedState, IgnoreReason, OutboundSyncResult, PhoebusObservedStatePolicy,
    SkipReason, SyncDirection, SyncOutcome,
};

#[test]
fn should_decide_malformed_phoebus_command_as_skipped_parse_error() {
    assert_eq!(
        decide_phoebus_command(
            "{ \"notRealCommandMessage\": \"Should not parse\" }",
            &PhoebusObservedStatePolicy::from_cache_entry(None),
        ),
        Err(PhoebusParseError::MalformedMessage)
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
            &PhoebusObservedStatePolicy::from_cache_entry(None),
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
            &PhoebusObservedStatePolicy::acknowledged(),
        ),
        Ok(PhoebusCommandDecision::DuplicateAcknowledgement)
    );
}

#[test]
fn should_decide_malformed_phoebus_config_as_skipped_parse_error() {
    assert_eq!(
        decide_phoebus_config(
            "{ \"notRealConfigMessage\": \"Should not parse\" }",
            &Config::default(),
        ),
        Err(PhoebusParseError::MalformedMessage)
    );
}

#[test]
fn should_decide_duplicate_phoebus_config() {
    let config = Config {
        enabled: Some(String::from("false")),
        user: String::from("test-user"),
        ..Config::default()
    };

    assert_eq!(
        decide_phoebus_config(&serde_json::to_string(&config).unwrap(), &config),
        Ok(PhoebusConfigDecision::DuplicateConfig {
            config: config.clone(),
        })
    );
}

#[test]
fn should_decide_config_with_same_enablement_as_metadata_only_update() {
    let cached_config = Config {
        enabled: Some(String::from("false")),
        user: String::from("cached-user"),
        ..Config::default()
    };
    let incoming_config = Config {
        enabled: Some(String::from("false")),
        user: String::from("incoming-user"),
        ..Config::default()
    };

    assert_eq!(
        decide_phoebus_config(
            &serde_json::to_string(&incoming_config).unwrap(),
            &cached_config,
        ),
        Ok(PhoebusConfigDecision::NoEnablementChange {
            config: incoming_config,
        })
    );
}

#[test]
fn should_decide_bypassed_phoebus_config_as_controls_bypass_or_snooze() {
    let cached_config = Config {
        enabled: Some(String::from("true")),
        ..Config::default()
    };
    let incoming_config = Config {
        enabled: Some(String::from("false")),
        user: String::from("test-user"),
        ..Config::default()
    };

    assert_eq!(
        decide_phoebus_config(
            &serde_json::to_string(&incoming_config).unwrap(),
            &cached_config,
        ),
        Ok(PhoebusConfigDecision::BypassOrSnooze {
            config: incoming_config,
            updated_state: CachedState {
                state: State::Bypassed,
                wake: None,
            },
        })
    );
}

#[test]
fn should_decide_active_phoebus_config_as_local_only_recording() {
    let cached_config = Config {
        enabled: Some(String::from("false")),
        ..Config::default()
    };
    let incoming_config = Config {
        enabled: Some(String::from("true")),
        user: String::from("test-user"),
        ..Config::default()
    };

    assert_eq!(
        decide_phoebus_config(
            &serde_json::to_string(&incoming_config).unwrap(),
            &cached_config,
        ),
        Ok(PhoebusConfigDecision::RecordActiveLocally {
            config: incoming_config,
            updated_state: CachedState {
                state: State::Ok,
                wake: None,
            },
        })
    );
}

#[test]
fn should_decide_invalid_enablement_phoebus_config_as_malformed() {
    let cached_config = Config {
        enabled: Some(String::from("false")),
        ..Config::default()
    };
    let incoming_config = Config {
        enabled: Some(String::from("not-a-real-enabled-value")),
        user: String::from("test-user"),
        ..Config::default()
    };

    assert_eq!(
        decide_phoebus_config(
            &serde_json::to_string(&incoming_config).unwrap(),
            &cached_config,
        ),
        Err(PhoebusParseError::MalformedMessage)
    );
}

#[test]
fn should_decide_acknowledgement_command_as_controls_acknowledge() {
    let command = Command {
        command: ACK_COMMAND.to_string(),
        user: String::from("test-user"),
        ..Command::default()
    };

    assert_eq!(
        decide_phoebus_command(
            &serde_json::to_string(&command).unwrap(),
            &PhoebusObservedStatePolicy::from_cache_entry(Some(ObservedAlarmState::new(
                CachedState {
                    state: State::Bypassed,
                    wake: None,
                },
            ))),
        ),
        Ok(PhoebusCommandDecision::Acknowledge {
            user: String::from("test-user"),
        })
    );
}

#[test]
fn should_extract_config_from_duplicate_config_decision() {
    let config = Config {
        enabled: Some(String::from("false")),
        user: String::from("test-user"),
        ..Config::default()
    };

    assert_eq!(
        PhoebusConfigDecision::DuplicateConfig {
            config: config.clone(),
        }
        .into_config(),
        config
    );
}

#[test]
fn should_extract_config_from_bypass_decision() {
    let config = Config {
        enabled: Some(String::from("false")),
        user: String::from("test-user"),
        ..Config::default()
    };

    assert_eq!(
        PhoebusConfigDecision::BypassOrSnooze {
            config: config.clone(),
            updated_state: CachedState::bypassed(),
        }
        .into_config(),
        config
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
        log_parse_outcome(
            "command",
            &key,
            "{ malformed",
            PhoebusParseError::MalformedMessage
        ),
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
        log_parse_outcome(
            "config",
            &key,
            "{ malformed",
            PhoebusParseError::MalformedMessage
        ),
        SyncOutcome::Skipped {
            reason: SkipReason::MalformedMessage,
        }
    );
}

#[test]
fn should_map_unsupported_monitor_key_to_noise_ignored_outcome() {
    assert_eq!(
        log_monitor_key_parse_outcome(
            "state:display/device",
            "{}",
            &KeyParseError::UnsupportedOperation,
        ),
        SyncOutcome::Ignored {
            reason: IgnoreReason::PhoebusNoise,
        }
    );
}

#[test]
fn should_map_malformed_monitor_key_to_skipped_outcome() {
    assert_eq!(
        log_monitor_key_parse_outcome("malformed-key", "{}", &KeyParseError::MalformedStructure),
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
            &PhoebusObservedStatePolicy::acknowledged(),
        ),
        Ok(PhoebusCommandDecision::DuplicateAcknowledgement)
    );
}

#[test]
fn should_treat_observed_bypass_state_as_duplicate_bypass_config() {
    let updated_state = ObservedAlarmState::new(CachedState::bypassed()).into_cached_state();

    assert!(ObservedAlarmState::new(CachedState::bypassed()).matches(&updated_state));
}

#[test]
fn should_treat_observed_acknowledged_state_as_effectively_active_for_config_policy() {
    let policy = PhoebusObservedStatePolicy::acknowledged();
    assert!(!policy.suppresses_bypass_duplicate(&CachedState::bypassed()));
    assert!(policy.is_already_active());
}

#[test]
fn should_map_empty_device_monitor_key_to_skipped_outcome() {
    assert_eq!(
        log_monitor_key_parse_outcome("command:display/", "{}", &KeyParseError::EmptyDevice),
        SyncOutcome::Skipped {
            reason: SkipReason::MalformedMessage,
        }
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

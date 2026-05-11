//! Tests for the Controls Transformations module.

use std::collections::HashMap;

use super::*;
use crate::models::generated::Timestamp;
use crate::models::phoebus::Config;

#[test]
fn should_convert_command() {
    let status = Status::default();
    let metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    let result_message = controls_to_phoebus(&status, Operation::Command, &metadata).unwrap();
    assert_eq!(result_message.key(), Some(String::from("command:/")));
    assert_eq!(
        result_message.value(),
        serde_json::to_string(&Command {
            user: String::new(),
            host: CONTROLS_HOST.to_string(),
            command: ACK_COMMAND.to_string(),
        })
        .unwrap()
    );
}

#[test]
fn should_convert_config() {
    let status = Status::default();
    let metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    let result_message = controls_to_phoebus(&status, Operation::Config, &metadata).unwrap();
    assert_eq!(result_message.key(), Some(String::from("config:/")));
    assert_eq!(
        result_message.value(),
        serde_json::to_string(&Config {
            enabled: Some(true.to_string()),
            host: CONTROLS_HOST.to_string(),
            phoebus_specific: metadata.phoebus_config_metadata.clone(),
            ..Config::default()
        })
        .unwrap()
    );
}

#[test]
fn should_map_bypassed_state() {
    let mut controls_alarm = Status::default();
    controls_alarm.set_state(State::Bypassed);
    let result = normalized_enablement_from_controls(&controls_alarm)
        .as_enabled_string()
        .unwrap();
    assert_eq!(result, false.to_string());
}

#[test]
fn should_map_enabled_state() {
    let controls_alarm = Status::default();
    let result = normalized_enablement_from_controls(&controls_alarm)
        .as_enabled_string()
        .unwrap();
    assert_eq!(result, true.to_string());
}

#[test]
fn should_map_snoozed_state() {
    let mut controls_alarm = Status::default();
    controls_alarm.set_state(State::Bypassed);
    controls_alarm.wake = Some(Timestamp {
        seconds: 0,
        nanos: 0,
    });

    let result = normalized_enablement_from_controls(&controls_alarm)
        .as_enabled_string()
        .unwrap();
    assert_eq!(
        result,
        Utc.timestamp_micros(0).single().unwrap().to_rfc3339()
    );
}

#[test]
fn should_not_transform_when_operation_is_state() {
    let status = Status::default();
    let metadata = PvMetadata {
        phoebus_config_metadata: HashMap::new(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    let result = controls_to_phoebus(&status, Operation::State, &metadata);
    assert!(result.is_err());
}

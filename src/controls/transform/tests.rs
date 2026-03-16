//! Tests for the Controls Transformations Module

use super::*;
use crate::models::generated::Timestamp;

#[test]
fn should_convert_command() {
    let status = Status::default();
    let metadata = PvMetadata {
        config: Config::default(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    let result_message = controls_to_phoebus(&status, Operation::Command, &metadata).unwrap();
    assert_eq!(result_message.key, Some(String::from("command:/")));
    assert_eq!(
        result_message.value,
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
        config: Config::default(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    let result_message = controls_to_phoebus(&status, Operation::Config, &metadata).unwrap();
    assert_eq!(result_message.key, Some(String::from("config:/")));
    assert_eq!(
        result_message.value,
        serde_json::to_string(&Config {
            enabled: Some(true.to_string()),
            host: CONTROLS_HOST.to_string(),
            ..metadata.config
        })
        .unwrap()
    );
}

#[test]
fn should_map_bypassed_state() {
    let mut controls_alarm = Status::default();
    controls_alarm.set_state(State::Bypassed);
    let result = get_enabled_string(&controls_alarm);
    assert_eq!(result, false.to_string());
}

#[test]
fn should_map_enabled_state() {
    let controls_alarm = Status::default();
    let result = get_enabled_string(&controls_alarm);
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

    let result = get_enabled_string(&controls_alarm);
    assert_eq!(
        result,
        Utc.timestamp_micros(0).single().unwrap().to_rfc3339()
    );
}

#[test]
fn should_not_transform_when_not_config_or_command() {
    let status = Status::default();
    let metadata = PvMetadata {
        config: Config::default(),
        display_path: String::new(),
        phoebus_topic: String::new(),
    };

    let result = controls_to_phoebus(&status, Operation::Other, &metadata).unwrap_err();
    assert_eq!(result, Operation::get_err_string_for_other());
}

#[test]
fn should_return_none_when_operation_is_other() {
    assert_eq!(
        get_topic_for_operation(
            &Operation::Other,
            &PvMetadata {
                config: Config::default(),
                display_path: String::default(),
                phoebus_topic: String::default()
            }
        ),
        None
    );
}

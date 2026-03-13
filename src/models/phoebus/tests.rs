//! Phoebus Models Module Tests

use super::*;
use crate::models::alarm::status::State;

#[test]
fn serde_json_deserializes_config_enabled_as_string() {
    let mut expected = Config::default();
    expected.enabled = Some(false.to_string());

    // First, test we get a successful output when the field is a raw Boolean
    let mut test_input = "{ \"user\": \"\", \"host\": \"\", \"enabled\": false }";
    let mut result = serde_json::from_str::<Config>(test_input).unwrap();
    assert_eq!(expected, result);

    // Next, test that we get the same output when the field is a string
    test_input = "{ \"user\": \"\", \"host\": \"\", \"enabled\": \"false\" }";
    result = serde_json::from_str::<Config>(test_input).unwrap();
    assert_eq!(expected, result);

    // Finally, test that we get the same output when the field is missing
    test_input = "{ \"user\": \"\", \"host\": \"\" }";
    result = serde_json::from_str::<Config>(test_input).unwrap();
    assert_eq!(Config::default(), result);
}

#[test]
fn should_create_key_from_string() {
    let result = Key::from("command:some/path/here/MyDevice".to_string());
    assert_eq!(result.device, "MyDevice");
    assert_eq!(result.display_path, "some/path/here");
    assert_eq!(result.operation, Operation::Command);

    let result = Key::from("config:some/other/path/here/MyDevice2".to_string());
    assert_eq!(result.device, "MyDevice2");
    assert_eq!(result.display_path, "some/other/path/here");
    assert_eq!(result.operation, Operation::Config);

    let result = Key::from("state:some/path/here/MyDevice".to_string());
    assert_eq!(result.device, "MyDevice");
    assert_eq!(result.display_path, "some/path/here");
    assert_eq!(result.operation, Operation::Other);
}

#[test]
fn should_get_cached_state_from_config() {
    let mut config = Config::default();
    let mut result = config.as_cached_state();
    assert_eq!(CachedState::bypassed(), result);

    config.enabled = Some(true.to_string());
    result = config.as_cached_state();
    assert_eq!(
        CachedState {
            state: State::Ok,
            wake: None
        },
        result
    );

    config.enabled = Some("Corrupted data".to_owned());
    result = config.as_cached_state();
    assert_eq!(CachedState::default(), result);

    config.enabled = Some("2000-01-01T00:00:00.000Z".to_owned());
    result = config.as_cached_state();
    assert_eq!(
        CachedState {
            state: State::Ok,
            wake: None
        },
        result
    );
}

#[test]
fn should_get_err_string_for_operation() {
    assert_eq!(
        "Cannot operate on type 'Other'",
        Operation::get_err_string_for_other()
    );
}

#[test]
fn should_get_operation_key_prefix() {
    assert_eq!("command", Operation::Command.get_key_prefix());
    assert_eq!("config", Operation::Config.get_key_prefix());
    assert_eq!("", Operation::Other.get_key_prefix());
}

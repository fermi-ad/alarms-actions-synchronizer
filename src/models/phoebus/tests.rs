//! Phoebus Models Module Tests

use super::*;
use crate::models::proto::common::alarm::status::State;

#[test]
fn build_metadata_for_unmapped_device_creates_metadata_with_normalized_topic() {
    let key = Key::parse("config:/path/to/alarm/device1").unwrap();

    // Test with Command suffix
    let metadata = PvMetadata::from_unmapped(&key, "testCommand");
    assert_eq!(metadata.phoebus_topic, "test");

    // Test without Command suffix
    let metadata = PvMetadata::from_unmapped(&key, "test-topic");
    assert_eq!(metadata.phoebus_topic, "test-topic");
}

#[test]
fn serde_json_deserializes_config_enabled_as_string() {
    let expected = Config {
        enabled: Some(false.to_string()),
        ..Config::default()
    };

    // First, test we get a successful output when the field is a raw Boolean
    let mut test_input = "{ \"user\": \"\", \"host\": \"\", \"enabled\": false }";
    let mut result = serde_json::from_str::<Config>(test_input).unwrap();
    assert_eq!(expected, result);

    // Next, test that we get the same output when the field is a string
    test_input = "{ \"user\": \"\", \"host\": \"\", \"enabled\": \"false\" }";
    result = serde_json::from_str::<Config>(test_input).unwrap();
    assert_eq!(expected, result);

    // Next, test that null is accepted and normalized to None
    test_input = "{ \"user\": \"\", \"host\": \"\", \"enabled\": null }";
    result = serde_json::from_str::<Config>(test_input).unwrap();
    assert_eq!(Config::default(), result);

    // Finally, test that we get the same output when the field is missing
    test_input = "{ \"user\": \"\", \"host\": \"\" }";
    result = serde_json::from_str::<Config>(test_input).unwrap();
    assert_eq!(Config::default(), result);
}

#[test]
fn should_convert_normalized_enablement_to_wire_string() {
    assert_eq!(
        NormalizedEnablement::Active.as_enabled_string(),
        Some(true.to_string())
    );
    assert_eq!(
        NormalizedEnablement::Bypassed.as_enabled_string(),
        Some(false.to_string())
    );

    let snooze = DateTime::parse_from_rfc3339("2000-01-01T00:00:00.000Z").unwrap();
    assert_eq!(
        NormalizedEnablement::SnoozedUntil(snooze).as_enabled_string(),
        Some(snooze.to_rfc3339())
    );
}

#[test]
fn should_parse_valid_key_strings() {
    let result = Key::parse("command:some/path/here/MyDevice").unwrap();
    assert_eq!(result.device, "MyDevice");
    assert_eq!(result.display_path, "some/path/here");
    assert_eq!(result.operation, Operation::Command);

    let result = Key::parse("config:some/other/path/here/MyDevice2").unwrap();
    assert_eq!(result.device, "MyDevice2");
    assert_eq!(result.display_path, "some/other/path/here");
    assert_eq!(result.operation, Operation::Config);

    let result = Key::parse("state:some/path/here/MyDevice").unwrap();
    assert_eq!(result.device, "MyDevice");
    assert_eq!(result.display_path, "some/path/here");
    assert_eq!(result.operation, Operation::State);
}

#[test]
fn should_reject_malformed_or_unsupported_keys() {
    assert_eq!(
        Key::parse("not recognizable key"),
        Err(KeyParseError::MalformedStructure)
    );
    assert_eq!(
        Key::parse("other:some/path/device"),
        Err(KeyParseError::UnsupportedOperation)
    );
    assert_eq!(Key::parse("config:/"), Err(KeyParseError::EmptyDevice));
}

#[test]
fn should_get_cached_state_from_config() {
    // A missing/null enabled field normalizes to Bypassed.
    let config = Config::default();
    let result = config.as_cached_state();
    assert_eq!(Ok(CachedState::bypassed()), result);

    // An explicit `true` enabled field normalizes to Unbypassed (active).
    let config = Config {
        enabled: Some(true.to_string()),
        ..Config::default()
    };
    let result = config.as_cached_state();
    assert_eq!(
        Ok(CachedState {
            state: State::Unbypassed,
            wake: None
        }),
        result
    );

    // A corrupted enabled field returns a parse error.
    let config = Config {
        enabled: Some("Corrupted data".to_owned()),
        ..Config::default()
    };
    let result = config.as_cached_state();
    assert_eq!(Err(PhoebusParseError::MalformedMessage), result);

    // A past RFC3339 timestamp normalizes to Unbypassed (the snooze has expired).
    let config = Config {
        enabled: Some("2000-01-01T00:00:00.000Z".to_owned()),
        ..Config::default()
    };
    let result = config.as_cached_state();
    assert_eq!(
        Ok(CachedState {
            state: State::Unbypassed,
            wake: None
        }),
        result
    );
}

#[test]
fn should_get_operation_key_prefix() {
    assert_eq!("command", Operation::Command.get_key_prefix());
    assert_eq!("config", Operation::Config.get_key_prefix());
    assert_eq!("state", Operation::State.get_key_prefix());
}

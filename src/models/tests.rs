//! Models Module Tests

use super::*;

#[test]
fn should_get_cached_state_from_alarm_status() {
    let status = alarm::Status {
        device: String::new(),
        source: alarm::status::Source::Analog as i32,
        state: alarm::status::State::Acknowledged as i32,
        severity: alarm::status::Severity::High as i32,
        acknowledgeable: false,
        time: None,
        epics_type: String::new(),
        user: String::new(),
        wake: None,
    };

    let result = CachedState::from(status);
    assert_eq!(result.state, alarm::status::State::Acknowledged);
    assert_eq!(result.wake, None);
}

#[test]
fn should_create_and_clone_sync_config() {
    let cancel_token = CancellationToken::new();
    let controls_host = String::from("my controls host");
    let controls_topic = String::from("my controls topic");
    let phoebus_host = String::from("my phoebus host");
    let phoebus_topics = vec![String::from("topic1"), String::from("topic2")];

    let orig_config = SynchronizerConfig::new(
        cancel_token.clone(),
        controls_host.clone(),
        controls_topic.clone(),
        phoebus_host.clone(),
        phoebus_topics.clone(),
    );

    assert_eq!(controls_host, orig_config.controls_host);
    assert_eq!(controls_topic, orig_config.controls_topic);
    assert_eq!(phoebus_host, orig_config.phoebus_host);
    assert_eq!(phoebus_topics, orig_config.phoebus_topics);
    assert_eq!(1, Arc::strong_count(&orig_config.alarm_states));
    assert_eq!(1, Arc::strong_count(&orig_config.pv_metadata));

    let cloned_config = orig_config.clone();
    assert_eq!(orig_config, cloned_config);

    cancel_token.cancel();
    assert!(orig_config.cancel_token.is_cancelled());
    assert!(cloned_config.cancel_token.is_cancelled());
}

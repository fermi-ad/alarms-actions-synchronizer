//! Models Module Tests

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::models::alarm::status::{Source, State};
use crate::models::cache::{ObservedAlarmState, read_observed_alarm_state};
use crate::models::generated::Timestamp;

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
fn should_match_observed_alarm_state_against_controls_status_by_state_and_wake() {
    let wake = Some(Timestamp {
        seconds: 123,
        nanos: 456,
    });
    let status = alarm::Status {
        device: String::from("device"),
        source: alarm::status::Source::Epics as i32,
        state: State::Bypassed as i32,
        wake: wake.clone(),
        ..alarm::Status::default()
    };

    let observed_state = ObservedAlarmState::from_status(&status);

    assert!(observed_state.matches(&status.clone().into()));
    assert!(!observed_state.matches(&CachedState {
        state: State::Bypassed,
        wake: None,
    }));
}

#[test]
fn should_build_acknowledged_observed_state_for_command_policy() {
    // PhoebusObservedStatePolicy::acknowledged() represents the acknowledged state
    // used for duplicate suppression of acknowledgement commands.
    let policy = PhoebusObservedStatePolicy::acknowledged();
    assert!(policy.suppresses_acknowledgement_duplicate());
    assert_eq!(
        policy.recorded_state(),
        Some(ObservedAlarmState::new(CachedState {
            state: State::Acknowledged,
            wake: None,
        }))
    );
}

#[test]
fn should_treat_acknowledged_observed_state_as_duplicate_for_command_policy() {
    let acked = PhoebusObservedStatePolicy::acknowledged();
    assert!(acked.suppresses_acknowledgement_duplicate());

    let ok_policy =
        PhoebusObservedStatePolicy::from_cache_entry(Some(ObservedAlarmState::new(CachedState {
            state: State::Ok,
            wake: None,
        })));
    assert!(!ok_policy.suppresses_acknowledgement_duplicate());

    let bypassed_policy = PhoebusObservedStatePolicy::from_cache_entry(Some(
        ObservedAlarmState::new(CachedState::bypassed()),
    ));
    assert!(!bypassed_policy.suppresses_acknowledgement_duplicate());
}

#[test]
fn should_treat_any_non_bypassed_observed_state_as_effectively_active_for_config_policy() {
    let acked = PhoebusObservedStatePolicy::acknowledged();
    assert!(acked.suppresses_active_record_for_current_config_policy());

    let ok_policy =
        PhoebusObservedStatePolicy::from_cache_entry(Some(ObservedAlarmState::new(CachedState {
            state: State::Ok,
            wake: None,
        })));
    assert!(ok_policy.suppresses_active_record_for_current_config_policy());

    let bypassed_policy = PhoebusObservedStatePolicy::from_cache_entry(Some(
        ObservedAlarmState::new(CachedState::bypassed()),
    ));
    assert!(!bypassed_policy.suppresses_active_record_for_current_config_policy());
}

#[test]
fn should_define_phoebus_acknowledgement_policy_duplicate_and_recorded_state() {
    let cached_entry = Some(ObservedAlarmState::new(CachedState {
        state: State::Acknowledged,
        wake: None,
    }));
    let policy = PhoebusObservedStatePolicy::from_cache_entry(cached_entry);

    assert!(policy.suppresses_acknowledgement_duplicate());
    assert_eq!(
        PhoebusObservedStatePolicy::acknowledged().recorded_state(),
        Some(ObservedAlarmState::new(CachedState {
            state: State::Acknowledged,
            wake: None,
        }))
    );
}

#[test]
fn should_build_phoebus_config_record_observed_state() {
    let wake = Some(Timestamp {
        seconds: 555,
        nanos: 1,
    });
    let updated_state = CachedState {
        state: State::Bypassed,
        wake: wake.clone(),
    };

    assert_eq!(
        PhoebusObservedStatePolicy::for_config_record(updated_state.clone()).recorded_state(),
        Some(ObservedAlarmState::new(updated_state.clone()))
    );
}

#[test]
fn should_define_phoebus_bypass_duplicate_by_exact_cached_state() {
    let wake = Some(Timestamp {
        seconds: 555,
        nanos: 1,
    });
    let policy =
        PhoebusObservedStatePolicy::from_cache_entry(Some(ObservedAlarmState::new(CachedState {
            state: State::Bypassed,
            wake: wake.clone(),
        })));

    assert!(policy.suppresses_bypass_duplicate(&CachedState {
        state: State::Bypassed,
        wake: wake.clone(),
    }));
    assert!(!policy.suppresses_bypass_duplicate(&CachedState {
        state: State::Bypassed,
        wake: None,
    }));
}

#[test]
fn should_preserve_active_state_asymmetry_in_phoebus_config_policy() {
    assert!(
        PhoebusObservedStatePolicy::from_cache_entry(Some(ObservedAlarmState::new(CachedState {
            state: State::Ok,
            wake: None,
        },)))
        .suppresses_active_record_for_current_config_policy()
    );
    assert!(
        PhoebusObservedStatePolicy::acknowledged()
            .suppresses_active_record_for_current_config_policy()
    );
    assert!(
        !PhoebusObservedStatePolicy::from_cache_entry(Some(ObservedAlarmState::new(
            CachedState::bypassed(),
        )))
        .suppresses_active_record_for_current_config_policy()
    );
    assert!(
        !PhoebusObservedStatePolicy::from_cache_entry(None)
            .suppresses_active_record_for_current_config_policy()
    );
}

#[test]
fn should_define_controls_duplicate_policy_by_exact_cached_state() {
    let wake = Some(Timestamp {
        seconds: 77,
        nanos: 3,
    });
    let status = alarm::Status {
        device: String::from("device"),
        source: Source::Epics as i32,
        state: State::Bypassed as i32,
        wake: wake.clone(),
        ..alarm::Status::default()
    };

    let policy = ControlsObservedStatePolicy::from_status(
        &status,
        Some(ObservedAlarmState::new(CachedState {
            state: State::Bypassed,
            wake: wake.clone(),
        })),
    );

    assert!(policy.suppresses_duplicate());
    assert!(
        !ControlsObservedStatePolicy::from_status(
            &status,
            Some(ObservedAlarmState::new(CachedState {
                state: State::Bypassed,
                wake: None,
            })),
        )
        .suppresses_duplicate()
    );
}

#[test]
fn should_define_controls_recorded_state_as_latest_incoming_observation() {
    let status = alarm::Status {
        device: String::from("device"),
        source: Source::Analog as i32,
        state: State::Acknowledged as i32,
        wake: None,
        ..alarm::Status::default()
    };

    let policy = ControlsObservedStatePolicy::from_status(
        &status,
        Some(ObservedAlarmState::new(CachedState {
            state: State::Ok,
            wake: None,
        })),
    );

    assert_eq!(policy.device(), "device");
    assert_eq!(
        policy.recorded_state(),
        ObservedAlarmState::from_status(&status)
    );
}

#[tokio::test]
async fn should_read_phoebus_observed_state_policy_from_latest_cache_entry() {
    let cache = Arc::new(RwLock::new(HashMap::from([(
        String::from("device"),
        CachedState {
            state: State::Bypassed,
            wake: None,
        },
    )])));

    let policy = read_phoebus_observed_state_policy(&cache, "device").await;

    assert!(policy.suppresses_bypass_duplicate(&CachedState::bypassed()));
    assert_eq!(
        policy.recorded_state(),
        Some(ObservedAlarmState::new(CachedState::bypassed()))
    );
}

#[tokio::test]
async fn should_record_phoebus_observed_state_from_policy_recorded_state() {
    let cache = Arc::new(RwLock::new(HashMap::new()));
    let policy = PhoebusObservedStatePolicy::acknowledged();

    record_phoebus_observed_state(&cache, "device", &policy).await;

    assert_eq!(
        read_observed_alarm_state(&cache, "device").await,
        Some(ObservedAlarmState::new(CachedState {
            state: State::Acknowledged,
            wake: None,
        }))
    );
}

#[test]
fn should_create_and_clone_sync_config() {
    let cancel_token = CancellationToken::new();
    let controls_host = String::from("my controls host");
    let controls_topic = String::from("my controls topic");
    let grpc_alarms_svc_host = String::from("grpc service host");
    let phoebus_host = String::from("my phoebus host");
    let phoebus_topics = vec![String::from("topic1"), String::from("topic2")];

    let orig_config = SynchronizerConfig::new(
        cancel_token.clone(),
        controls_host.clone(),
        controls_topic.clone(),
        grpc_alarms_svc_host.clone(),
        phoebus_host.clone(),
        phoebus_topics.clone(),
    );

    assert_eq!(controls_host, orig_config.controls_host);
    assert_eq!(controls_topic, orig_config.controls_topic);
    assert_eq!(grpc_alarms_svc_host, orig_config.grpc_alarms_svc_host);
    assert_eq!(phoebus_host, orig_config.phoebus_host);
    assert_eq!(phoebus_topics, orig_config.phoebus_topics);
    assert_eq!(1, Arc::strong_count(&orig_config.alarm_states));

    let cloned_config = orig_config.clone();
    assert_eq!(orig_config, cloned_config);

    cancel_token.cancel();
    assert!(orig_config.cancel_token.is_cancelled());
    assert!(cloned_config.cancel_token.is_cancelled());
}

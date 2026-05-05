//! Models Module Tests

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::models::alarm::status::{Source, State};
use crate::models::cache::read_observed_state_policy;
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

    let result = CachedState::from(&status);
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

    let observed_state = CachedState::from(&status);

    assert_eq!(observed_state, CachedState::from(&status));
    assert_ne!(
        observed_state,
        CachedState {
            state: State::Bypassed,
            wake: None,
        }
    );
}

#[test]
fn should_build_acknowledged_observed_state_for_command_policy() {
    // An ObservedStatePolicy with an acknowledged cached state suppresses an incoming
    // acknowledged state (exact match → duplicate suppression).
    let policy = ObservedStatePolicy::new(Some(CachedState {
        state: State::Acknowledged,
        wake: None,
    }));
    let incoming_ack = CachedState {
        state: State::Acknowledged,
        wake: None,
    };
    assert!(policy.suppresses_incoming(&incoming_ack));
}

#[test]
fn should_treat_acknowledged_observed_state_as_duplicate_for_command_policy() {
    // An already-acknowledged observed state suppresses a new incoming acknowledgement.
    let acked = ObservedStatePolicy::new(Some(CachedState {
        state: State::Acknowledged,
        wake: None,
    }));
    assert!(acked.suppresses_incoming(&CachedState {
        state: State::Acknowledged,
        wake: None,
    }));

    // An Ok observed state does not suppress an incoming acknowledgement.
    let ok_policy = ObservedStatePolicy::new(Some(CachedState {
        state: State::Ok,
        wake: None,
    }));
    assert!(!ok_policy.suppresses_incoming(&CachedState {
        state: State::Acknowledged,
        wake: None,
    }));

    // A Bypassed observed state suppresses an incoming Acknowledged state because
    // transitioning from Bypassed to Acknowledged is forbidden by policy (only Bypassed
    // and Unbypassed transitions are allowed out of Bypassed).
    let bypassed_policy = ObservedStatePolicy::new(Some(CachedState::bypassed()));
    assert!(bypassed_policy.suppresses_incoming(&CachedState {
        state: State::Acknowledged,
        wake: None,
    }));
}

#[test]
fn should_treat_bypassed_observed_state_as_the_only_state_that_allows_unbypassed_transition() {
    // Only a Bypassed observed state allows an incoming Unbypassed (active) transition.
    // Non-bypassed states (Ok, Acknowledged) do NOT suppress Unbypassed because they are
    // not equal to Unbypassed and is_transition_forbidden only applies when observed is Bypassed.
    let acked = ObservedStatePolicy::new(Some(CachedState {
        state: State::Acknowledged,
        wake: None,
    }));
    assert!(!acked.suppresses_incoming(&CachedState {
        state: State::Unbypassed,
        wake: None,
    }));

    let ok_policy = ObservedStatePolicy::new(Some(CachedState {
        state: State::Ok,
        wake: None,
    }));
    assert!(!ok_policy.suppresses_incoming(&CachedState {
        state: State::Unbypassed,
        wake: None,
    }));

    // A Bypassed observed state does NOT suppress an incoming Unbypassed state
    // (Unbypassed is an explicitly allowed transition out of Bypassed).
    let bypassed_policy = ObservedStatePolicy::new(Some(CachedState::bypassed()));
    assert!(!bypassed_policy.suppresses_incoming(&CachedState {
        state: State::Unbypassed,
        wake: None,
    }));
}

#[test]
fn should_define_phoebus_acknowledgement_policy_duplicate_and_recorded_state() {
    let cached_entry = Some(CachedState {
        state: State::Acknowledged,
        wake: None,
    });
    let policy = ObservedStatePolicy::new(cached_entry);

    assert!(policy.suppresses_incoming(&CachedState {
        state: State::Acknowledged,
        wake: None,
    }));
}

#[test]
fn should_define_phoebus_bypass_duplicate_by_exact_cached_state() {
    let wake = Some(Timestamp {
        seconds: 555,
        nanos: 1,
    });
    let policy = ObservedStatePolicy::new(Some(CachedState {
        state: State::Bypassed,
        wake: wake.clone(),
    }));

    assert!(policy.suppresses_incoming(&CachedState {
        state: State::Bypassed,
        wake: wake.clone(),
    }));
    assert!(!policy.suppresses_incoming(&CachedState {
        state: State::Bypassed,
        wake: None,
    }));
}

#[test]
fn should_preserve_active_state_asymmetry_in_phoebus_config_policy() {
    // Non-bypassed states (Ok, Acknowledged) do NOT suppress an incoming Unbypassed state
    // because they are not equal to Unbypassed and is_transition_forbidden only applies
    // when the observed state is Bypassed.
    assert!(
        !ObservedStatePolicy::new(Some(CachedState {
            state: State::Ok,
            wake: None,
        },))
        .suppresses_incoming(&CachedState {
            state: State::Unbypassed,
            wake: None,
        })
    );
    assert!(
        !ObservedStatePolicy::new(Some(CachedState {
            state: State::Acknowledged,
            wake: None,
        }))
        .suppresses_incoming(&CachedState {
            state: State::Unbypassed,
            wake: None,
        })
    );
    // Bypassed observed state does NOT suppress Unbypassed (explicitly allowed transition).
    assert!(
        !ObservedStatePolicy::new(Some(CachedState::bypassed(),)).suppresses_incoming(
            &CachedState {
                state: State::Unbypassed,
                wake: None,
            }
        )
    );
    // No observed state means nothing is suppressed.
    assert!(
        !ObservedStatePolicy::new(None).suppresses_incoming(&CachedState {
            state: State::Unbypassed,
            wake: None,
        })
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

    let incoming = CachedState::from(&status);

    let policy_matching = ObservedStatePolicy::new(Some(CachedState {
        state: State::Bypassed,
        wake: wake.clone(),
    }));
    assert!(policy_matching.suppresses_incoming(&incoming));

    let policy_different_wake = ObservedStatePolicy::new(Some(CachedState {
        state: State::Bypassed,
        wake: None,
    }));
    assert!(!policy_different_wake.suppresses_incoming(&incoming));
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

    let incoming = CachedState::from(&status);
    assert_eq!(incoming.state, State::Acknowledged);
    assert_eq!(incoming.wake, None);
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

    let policy = read_observed_state_policy(&cache, "device").await;

    assert!(policy.suppresses_incoming(&CachedState::bypassed()));
}

#[tokio::test]
async fn should_record_phoebus_observed_state_from_policy_recorded_state() {
    let cache = Arc::new(RwLock::new(HashMap::new()));

    record_alarm_state(
        &cache,
        "device",
        CachedState {
            state: State::Acknowledged,
            wake: None,
        },
    )
    .await;

    assert_eq!(
        cache.read().await.get("device").cloned(),
        Some(CachedState {
            state: State::Acknowledged,
            wake: None,
        })
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

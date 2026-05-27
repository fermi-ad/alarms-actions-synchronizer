//! Tests for the alarm state cache module.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use super::*;
use crate::models::proto::common::alarm::status::{Source, State};
use crate::models::proto::google::protobuf::Timestamp;

fn make_cache() -> AlarmStateCache {
    Arc::new(RwLock::new(HashMap::new()))
}

// --- record_state_hydrated_state ---

#[tokio::test]
async fn should_write_state_when_no_existing_entry() {
    let cache = make_cache();
    let state = CachedState {
        state: State::Ok,
        wake: None,
    };

    record_startup_state_evidence(&cache, "MyDevice", state.clone()).await;

    let stored = cache.read().await.get("MyDevice").cloned();
    assert_eq!(stored, Some(state));
}

#[tokio::test]
async fn should_preserve_snoozed_entry_when_state_record_conflicts() {
    let cache = make_cache();

    // Seed a snoozed (Bypassed + wake) entry, as if a config record was processed first.
    let snooze_wake = Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    };
    let snoozed = CachedState {
        state: State::Bypassed,
        wake: Some(snooze_wake),
    };
    cache
        .write()
        .await
        .insert("MyDevice".to_owned(), snoozed.clone());

    // Now a state record arrives with a conflicting Ok state.
    let conflicting = CachedState {
        state: State::Ok,
        wake: None,
    };
    record_startup_state_evidence(&cache, "MyDevice", conflicting).await;

    // The snoozed entry must be preserved; the state record must not overwrite it.
    let stored = cache.read().await.get("MyDevice").cloned();
    assert_eq!(stored, Some(snoozed));
}

#[tokio::test]
async fn should_preserve_bypassed_entry_without_wake_when_state_record_conflicts() {
    let cache = make_cache();

    // Seed a plain Bypassed (no wake) entry from a config record.
    let bypassed = CachedState {
        state: State::Bypassed,
        wake: None,
    };
    cache
        .write()
        .await
        .insert("MyDevice".to_owned(), bypassed.clone());

    // A state record arrives with a conflicting Ok state.
    let conflicting = CachedState {
        state: State::Ok,
        wake: None,
    };
    record_startup_state_evidence(&cache, "MyDevice", conflicting).await;

    // The Bypassed entry must be preserved.
    let stored = cache.read().await.get("MyDevice").cloned();
    assert_eq!(stored, Some(bypassed));
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
        wake,
    }));

    assert!(policy.suppresses_incoming(&CachedState {
        state: State::Bypassed,
        wake,
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
    let status = Status {
        device: String::from("device"),
        source: Source::Epics as i32,
        state: State::Bypassed as i32,
        wake,
        ..Status::default()
    };

    let incoming = CachedState::from(&status);

    let policy_matching = ObservedStatePolicy::new(Some(CachedState {
        state: State::Bypassed,
        wake,
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
    let status = Status {
        device: String::from("device"),
        source: Source::Analog as i32,
        state: State::Acknowledged as i32,
        wake: None,
        ..Status::default()
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
    let cache = make_cache();

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

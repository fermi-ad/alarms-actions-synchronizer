//! Tests for the alarm state cache module.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use super::*;
use crate::models::alarm::status::State;
use crate::models::generated::Timestamp;

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

    record_state_hydrated_state(&cache, "MyDevice", state.clone()).await;

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
        wake: Some(snooze_wake.clone()),
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
    record_state_hydrated_state(&cache, "MyDevice", conflicting).await;

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
    record_state_hydrated_state(&cache, "MyDevice", conflicting).await;

    // The Bypassed entry must be preserved.
    let stored = cache.read().await.get("MyDevice").cloned();
    assert_eq!(stored, Some(bypassed));
}

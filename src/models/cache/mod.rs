//! Alarm state cache types and policy helpers.
//!
//! Contains the shared cache type alias, the `CachedState` value type, and the
//! observed-state policy helpers used for duplicate suppression and loop prevention.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::RwLock;
use tracing::debug;

use crate::models::alarm::Status;
use crate::models::alarm::status::State;
use crate::models::generated::Timestamp;

#[cfg(test)]
mod tests;

/// Alias for the atomic cache of alarm state data, shared between the two synchronizing processes.
///
/// This cache is the synchronizer's local memory of the latest in-scope alarm-handling state it has
/// observed for each EPICS device. It is intentionally used for duplicate suppression and loop avoidance,
/// even when an outbound publish or RPC fails.
///
/// The values in this cache therefore mean "latest observed by the synchronizer" rather than
/// "latest confirmed mirrored to the opposite system".
pub type AlarmStateCache = Arc<RwLock<HashMap<String, CachedState>>>;

/// Encapsulates the latest state information about an alarm.
#[derive(Clone, Debug, PartialEq)]
pub struct CachedState {
    /// The latest [`State`] the sync service has recorded for the alarm.
    ///
    /// This is the latest in-scope state the synchronizer has observed locally, not necessarily the latest
    /// state that has been confirmed as mirrored successfully to the opposite system.
    pub state: State,
    /// If the alarm is snoozed, this field will be set to [`Some`] with the reenablement time, and
    /// the [`state`](Self::state) field will be set to [`Bypassed`](State::Bypassed).
    /// Otherwise, this field will be [`None`].
    pub wake: Option<Timestamp>,
}

impl CachedState {
    /// Creates a [`CachedState`] representing an indefinitely bypassed alarm with no scheduled wake time.
    pub fn bypassed() -> Self {
        Self {
            state: State::Bypassed,
            wake: None,
        }
    }
}

impl Default for CachedState {
    /// Returns a [`CachedState`] with [`State::Unknown`] and no wake time, representing an unobserved alarm.
    fn default() -> Self {
        Self {
            state: State::Unknown,
            wake: None,
        }
    }
}

impl From<&Status> for CachedState {
    /// Converts a Controls [`Status`] into a [`CachedState`] by extracting its state and wake fields.
    fn from(value: &Status) -> Self {
        CachedState {
            state: value.state(),
            wake: value.wake,
        }
    }
}

impl From<bool> for CachedState {
    /// Converts a boolean enablement flag into a [`CachedState`].
    ///
    /// `true` maps to [`State::Unbypassed`]; `false` maps to [`State::Bypassed`] with no wake time.
    fn from(is_active: bool) -> Self {
        Self {
            state: if is_active {
                State::Unbypassed
            } else {
                State::Bypassed
            },
            wake: None,
        }
    }
}

impl<Tz: TimeZone> From<DateTime<Tz>> for CachedState {
    /// Converts a datetime into a [`CachedState`].
    ///
    /// If the datetime is in the future, the state is [`State::Bypassed`] with the wake time set.
    /// If the datetime is in the past or present, the state is [`State::Unbypassed`] with no wake time.
    fn from(value: DateTime<Tz>) -> Self {
        if value.timestamp_millis() > Utc::now().timestamp_millis() {
            Self {
                state: State::Bypassed,
                wake: Some(Timestamp {
                    seconds: value.timestamp(),
                    nanos: value.timestamp_subsec_nanos() as i32,
                }),
            }
        } else {
            Self {
                state: State::Unbypassed,
                wake: None,
            }
        }
    }
}

/// Encapsulates the latest observed state for a device and provides policy-level suppression logic.
///
/// Used to prevent duplicate synchronization attempts and to guard against forbidden state transitions
/// (e.g., a bypassed device being re-alarmed without an explicit unbypass).
#[derive(Clone, Debug, PartialEq)]
pub struct ObservedStatePolicy {
    observed: Option<CachedState>,
}

impl ObservedStatePolicy {
    /// Creates a new [`ObservedStatePolicy`] from the latest observed [`CachedState`] for a device, if any.
    pub fn new(observed: Option<CachedState>) -> Self {
        Self { observed }
    }

    /// Returns `true` if the observed state should suppress the incoming state.
    ///
    /// Suppression occurs when the incoming state is identical to the observed state, or when
    /// the transition from the observed state to the incoming state is forbidden by policy.
    pub fn suppresses_incoming(&self, incoming: &CachedState) -> bool {
        self.observed
            .as_ref()
            .is_some_and(|observed| observed_suppresses_incoming(observed, incoming))
    }
}

/// Creates the shared observed-state policy for an incoming alarm.
pub async fn read_observed_state_policy(
    cache: &AlarmStateCache,
    device: &str,
) -> ObservedStatePolicy {
    let observed = cache.read().await.get(device).cloned();
    ObservedStatePolicy::new(observed)
}

/// Records the latest observed alarm state for `device`.
pub async fn record_alarm_state(cache: &AlarmStateCache, device: &str, state: CachedState) {
    cache.write().await.insert(device.to_owned(), state);
}

/// Records a startup-hydrated alarm state derived from a Phoebus state record, preserving any
/// existing config-derived bypass/snooze entry.
///
/// State records are secondary startup evidence. If the cache already holds a `Bypassed` entry
/// (written by a config record), this write is skipped so the stronger config-derived semantics
/// are not erased by weaker state-record evidence.
pub async fn record_startup_state_evidence(
    cache: &AlarmStateCache,
    device: &str,
    state: CachedState,
) {
    let mut writer = cache.write().await;
    if writer
        .get(device)
        .is_some_and(|existing| existing.state == State::Bypassed)
    {
        debug!(
            device = device,
            "Preserving config-derived startup bypass/snooze state for device instead of overwriting it with secondary Phoebus state-record evidence."
        );
        return;
    }
    writer.insert(device.to_owned(), state);
}

/// Returns `true` if the observed state should suppress the incoming state.
///
/// Suppression occurs when the two states are equal (duplicate) or when the transition is forbidden.
fn observed_suppresses_incoming(observed: &CachedState, incoming: &CachedState) -> bool {
    observed == incoming || is_transition_forbidden(observed, incoming)
}

/// Guards against a device coming out of bypass unless the new state us an updated bypass (i.e., the timer on a snoozed alarm was changed)
/// or the device was explicitly unbypassed.
fn is_transition_forbidden(observed: &CachedState, incoming: &CachedState) -> bool {
    observed.state == State::Bypassed
        && match incoming.state {
            State::Bypassed | State::Unbypassed => false,
            _ => true,
        }
}

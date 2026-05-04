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
    pub fn bypassed() -> Self {
        Self {
            state: State::Bypassed,
            wake: None,
        }
    }

    /// Creates a cached state value from a Controls [`Status`].
    pub fn from_status(status: &Status) -> Self {
        status.clone().into()
    }

    /// Returns whether this state matches the provided incoming state exactly for duplicate suppression.
    pub fn matches(&self, incoming: &CachedState) -> bool {
        self == incoming
    }
}

impl Default for CachedState {
    fn default() -> Self {
        Self {
            state: State::Unknown,
            wake: None,
        }
    }
}

impl From<Status> for CachedState {
    fn from(value: Status) -> Self {
        CachedState {
            state: value.state(),
            wake: value.wake,
        }
    }
}

impl From<bool> for CachedState {
    fn from(is_active: bool) -> Self {
        Self {
            state: if is_active {
                State::Ok
            } else {
                State::Bypassed
            },
            wake: None,
        }
    }
}

impl<Tz: TimeZone> From<DateTime<Tz>> for CachedState {
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
                state: State::Ok,
                wake: None,
            }
        }
    }
}

/// Tiny shared policy surface for Controls-originated inbound intent.
#[derive(Clone, Debug, PartialEq)]
pub struct ControlsObservedStatePolicy {
    device: String,
    incoming: CachedState,
    observed: Option<CachedState>,
}

impl ControlsObservedStatePolicy {
    /// Creates a policy for an incoming Controls alarm and the current observed-state cache entry, if any.
    pub fn from_status(status: &Status, observed: Option<CachedState>) -> Self {
        Self {
            device: status.device.clone(),
            incoming: CachedState::from_status(status),
            observed,
        }
    }

    /// Returns the device whose observed-state semantics are being evaluated.
    pub fn device(&self) -> &str {
        &self.device
    }

    /// Returns whether the current observed cache entry suppresses this Controls update as a duplicate.
    pub fn suppresses_duplicate(&self) -> bool {
        self.observed
            .as_ref()
            .is_some_and(|observed| observed.matches(&self.incoming))
    }

    /// Returns the latest-observed state that should be recorded after processing this Controls update.
    ///
    /// Controls preserves the current latest-observed incoming state locally for duplicate suppression and loop
    /// prevention, including after local-only handling and after attempted outbound sync regardless of transport
    /// result.
    pub fn recorded_state(&self) -> CachedState {
        self.incoming.clone()
    }
}

/// Tiny shared policy surface for Phoebus-originated inbound intent.
#[derive(Clone, Debug, PartialEq)]
pub struct PhoebusObservedStatePolicy {
    observed: Option<CachedState>,
}

impl PhoebusObservedStatePolicy {
    /// Creates a policy for the current observed-state cache entry, if any.
    pub fn from_cache_entry(observed: Option<CachedState>) -> Self {
        Self { observed }
    }

    /// Returns the shared policy representing an acknowledgement command accepted from Phoebus.
    pub fn acknowledged() -> Self {
        Self::from_cache_entry(Some(CachedState {
            state: State::Acknowledged,
            wake: None,
        }))
    }

    /// Returns the shared policy representing a Phoebus config update whose latest observed state should be stored.
    pub fn for_config_record(updated_state: CachedState) -> Self {
        Self::from_cache_entry(Some(updated_state))
    }

    /// Returns whether the current observed cache entry suppresses a repeated acknowledgement command.
    pub fn suppresses_acknowledgement_duplicate(&self) -> bool {
        self.observed
            .as_ref()
            .is_some_and(|observed| observed.state == State::Acknowledged)
    }

    /// Returns whether the current observed cache entry suppresses a repeated bypass/snooze config intent.
    pub fn suppresses_bypass_duplicate(&self, incoming: &CachedState) -> bool {
        self.observed
            .as_ref()
            .is_some_and(|observed| observed.matches(incoming))
    }

    /// Returns whether the current observed cache entry should treat Phoebus re-activation as already recorded
    /// for the current asymmetric config policy.
    pub fn suppresses_activation_duplicate(&self) -> bool {
        self.observed
            .as_ref()
            .is_some_and(|observed| observed.state != State::Bypassed)
    }

    /// Returns the latest-observed state that should be recorded after accepting this Phoebus-side intent,
    /// or `None` if this policy has no recordable state.
    pub fn recorded_state(&self) -> Option<CachedState> {
        self.observed.clone()
    }
}

/// Reads the latest observed alarm state for `device`, if any.
pub async fn read_observed_alarm_state(
    cache: &AlarmStateCache,
    device: &str,
) -> Option<CachedState> {
    cache.read().await.get(device).cloned()
}

/// Creates the shared Controls observed-state policy for an incoming alarm.
pub async fn read_controls_observed_state_policy(
    cache: &AlarmStateCache,
    status: &Status,
) -> ControlsObservedStatePolicy {
    ControlsObservedStatePolicy::from_status(
        status,
        read_observed_alarm_state(cache, &status.device).await,
    )
}

/// Creates the shared Phoebus observed-state policy for a device from the latest observed cache entry.
pub async fn read_phoebus_observed_state_policy(
    cache: &AlarmStateCache,
    device: &str,
) -> PhoebusObservedStatePolicy {
    PhoebusObservedStatePolicy::from_cache_entry(read_observed_alarm_state(cache, device).await)
}

/// Records the latest observed alarm state for `device`.
pub async fn record_observed_alarm_state(
    cache: &AlarmStateCache,
    device: &str,
    observed_state: CachedState,
) {
    cache
        .write()
        .await
        .insert(device.to_owned(), observed_state);
}

/// Records a startup-hydrated alarm state derived from a Phoebus config record.
///
/// Config records are the authoritative source for bypass/snooze semantics and always overwrite any
/// previously cached entry for the device.
pub async fn record_config_hydrated_state(
    cache: &AlarmStateCache,
    device: &str,
    state: CachedState,
) {
    cache.write().await.insert(device.to_owned(), state);
}

/// Records a startup-hydrated alarm state derived from a Phoebus state record, preserving any
/// existing config-derived bypass/snooze entry.
///
/// State records are secondary startup evidence. If the cache already holds a `Bypassed` entry
/// (written by a config record), this write is skipped so the stronger config-derived semantics
/// are not erased by weaker state-record evidence.
pub async fn record_state_hydrated_state(
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

/// Records the latest observed Controls state according to the shared Controls policy surface.
pub async fn record_controls_observed_state(
    cache: &AlarmStateCache,
    policy: &ControlsObservedStatePolicy,
) {
    record_observed_alarm_state(cache, policy.device(), policy.recorded_state()).await;
}

/// Records the latest observed Phoebus state according to the shared Phoebus policy surface.
pub async fn record_phoebus_observed_state(
    cache: &AlarmStateCache,
    device: &str,
    policy: &PhoebusObservedStatePolicy,
) {
    if let Some(state) = policy.recorded_state() {
        record_observed_alarm_state(cache, device, state).await;
    }
}

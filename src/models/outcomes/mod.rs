//! Synchronization outcome types.
//!
//! Contains the structured result types used to describe what happened during
//! a synchronization attempt, including duplicate suppression, out-of-scope filtering,
//! skipped work, attempted propagation, and startup hydration.

/// Structured summary of what happened while processing a synchronization-relevant event.
///
/// The variants are designed to distinguish duplicate suppression, out-of-scope filtering, skipped work,
/// attempted propagation, and startup hydration. Stage 1 uses this model to make synchronization semantics
/// explicit without yet changing the existing anti-loop cache-write policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The inbound value matched the synchronizer's latest observed state, so nothing was mirrored.
    Duplicate,

    /// The message was intentionally ignored because it carries no synchronization-relevant user intent.
    Ignored { reason: IgnoreReason },

    /// The device is currently out of scope because Phoebus configuration metadata is not known.
    OutOfScope { reason: OutOfScopeReason },

    /// A synchronization attempt was skipped even though the message was otherwise relevant.
    Skipped { reason: SkipReason },

    /// The synchronizer attempted propagation to the opposite system.
    Attempted {
        direction: SyncDirection,
        result: AttemptResult,
    },

    /// State or metadata was hydrated from startup evidence rather than runtime user intent.
    Hydrated,
}

/// The direction in which synchronization was attempted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncDirection {
    ControlsToPhoebus,
    PhoebusToControls,
}

/// Why an inbound message was ignored without any synchronization attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IgnoreReason {
    /// The device belongs to a non-EPICS source and is outside this synchronizer's mission.
    ExternalSource,
    /// The message class is part of Phoebus traffic but does not express user intent this service mirrors.
    UnsupportedOperation,
    /// A Phoebus message class was recognized as server-state or otherwise irrelevant noise.
    PhoebusNoise,
}

/// Why a device was treated as out of scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutOfScopeReason {
    /// No Phoebus configuration metadata is known for this EPICS device yet.
    MissingPhoebusMetadata,
}

/// Why a synchronization-relevant action was skipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// No topic could be resolved for the requested Phoebus operation.
    MissingTopic,
    /// No publisher/client capability existed for the resolved destination.
    MissingPublisher,
    /// An upstream API or feature is intentionally unavailable in this repository.
    UnsupportedCapability,
    /// Input was malformed for the expected message class.
    MalformedMessage,
}

/// Result of attempting synchronization work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptResult {
    Succeeded,
    Failed,
}

/// What happened while trying to send a synchronization update to the opposite system.
///
/// Regardless of whether the transport reported success, failure, or a skip condition, the synchronizer has
/// already observed the inbound user intent and therefore refreshes its local latest-observed cache at the call
/// sites to preserve duplicate suppression and loop prevention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboundSyncResult {
    /// The synchronizer attempted to publish or send the update and the transport reported success.
    Succeeded,
    /// The synchronizer attempted to publish or send the update and the transport reported failure.
    Failed,
    /// The synchronizer could not attempt the outbound update because no destination capability existed.
    Skipped { reason: SkipReason },
}

impl OutboundSyncResult {
    /// Maps an outbound transport result into the higher-level synchronization outcome model.
    pub fn into_sync_outcome(self, direction: SyncDirection) -> SyncOutcome {
        match self {
            Self::Succeeded => SyncOutcome::Attempted {
                direction,
                result: AttemptResult::Succeeded,
            },
            Self::Failed => SyncOutcome::Attempted {
                direction,
                result: AttemptResult::Failed,
            },
            Self::Skipped { reason } => SyncOutcome::Skipped { reason },
        }
    }
}

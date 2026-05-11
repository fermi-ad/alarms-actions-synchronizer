//! Metadata and Scope Abstraction
//!
//! This module provides a focused abstraction around PV metadata and scope discovery.
//!
//! The abstraction centralizes the logic for:
//! - Looking up PV metadata by device name
//! - Determining whether a device is in scope for synchronization
//! - Creating metadata from Phoebus configuration (runtime discovery)
//! - Updating cached metadata
//!
//! Design principles:
//! - Keep the abstraction small and domain-specific
//! - Do not merge with alarm-state cache policy
//! - Preserve the current meaning of missing metadata as "out of scope"
//! - Preserve the current runtime ability for Phoebus config traffic to bring devices into scope

use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::models::phoebus::PvMetadata;

#[cfg(test)]
mod tests;

/// A focused abstraction around PV metadata and scope discovery.
///
/// This struct provides operations for:
/// - [`lookup_metadata_by_device()`](MetadataScope::lookup_metadata_by_device) - Get metadata for a device, if available
/// - [`update_cached_metadata()`](MetadataScope::update_cached_metadata) - Update the metadata cache
///
/// The abstraction preserves the current semantics:
/// - Missing metadata means "out of scope" for Controls-driven synchronization
/// - Runtime Phoebus config traffic can bring devices into scope
/// - Topic normalization around "Command" suffix handling
#[derive(Clone, Debug)]
pub struct MetadataScope {
    /// The atomic cache of PV metadata.
    metadata_cache: Arc<RwLock<HashMap<String, PvMetadata>>>,
}

impl MetadataScope {
    /// Creates a new [`MetadataScope`] from the shared PvCache.
    pub fn new() -> Self {
        Self {
            metadata_cache: Arc::new(RwLock::default()),
        }
    }

    /// Looks up the PV metadata for the given device.
    ///
    /// Returns `Some(PvMetadata)` if the device has metadata in the cache,
    /// or `None` if the device is not in scope (no metadata available).
    ///
    /// This is the primary method for determining whether a device should be
    /// processed for synchronization. Missing metadata means the device is
    /// currently out of scope.
    pub async fn lookup_metadata_by_device(&self, device: &str) -> Option<PvMetadata> {
        self.metadata_cache.read().await.get(device).cloned()
    }

    /// Updates the cached metadata for a device.
    ///
    /// This method is used to persist new metadata (from runtime discovery)
    /// or updated metadata (from config changes) in the shared cache.
    ///
    /// # Arguments
    ///
    /// - `device`: The device name (PV name)
    /// - `new_metadata`: The new or updated metadata to store
    pub async fn update_cached_metadata(&self, device: &str, new_metadata: PvMetadata) {
        self.metadata_cache
            .write()
            .await
            .insert(device.to_string(), new_metadata);
    }
}

impl PartialEq for MetadataScope {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.metadata_cache, &other.metadata_cache)
    }
}

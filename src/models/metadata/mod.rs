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

use crate::models::phoebus::{Config, PvMetadata};

#[cfg(test)]
mod tests;

/// A focused abstraction around PV metadata and scope discovery.
///
/// This struct provides operations for:
/// - [`lookup_metadata_by_device()`](MetadataScope::lookup_metadata_by_device) - Get metadata for a device, if available
/// - [`discover_metadata_from_config()`](MetadataScope::discover_metadata_from_config) - Create metadata from Phoebus config
/// - [`update_cached_metadata()`](MetadataScope::update_cached_metadata) - Update the metadata cache
///
/// The abstraction preserves the current semantics:
/// - Missing metadata means "out of scope" for Controls-driven synchronization
/// - Runtime Phoebus config traffic can bring devices into scope
/// - Topic normalization around "Command" suffix handling
#[derive(Clone, Debug)]
pub struct MetadataScope {
    /// The atomic cache of PV metadata.
    pv_metadata: Arc<RwLock<HashMap<String, PvMetadata>>>,
}

impl MetadataScope {
    /// Creates a new [`MetadataScope`] from the shared PvCache.
    pub fn new() -> Self {
        Self {
            pv_metadata: Arc::new(RwLock::default()),
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
        self.pv_metadata.read().await.get(device).cloned()
    }

    /// Creates PV metadata from a Phoebus configuration message.
    ///
    /// This method is used when Phoebus config traffic discovers a new device
    /// at runtime. The metadata includes:
    /// - The configuration from the message
    /// - The display path extracted from the key
    /// - The normalized topic name (stripping "Command" suffix if present)
    ///
    /// # Arguments
    ///
    /// - `key`: The parsed Phoebus Kafka key containing operation, display_path, and device
    /// - `config`: The configuration from the Phoebus message
    /// - `topic`: The Phoebus topic name (will be normalized by stripping "Command" suffix)
    ///
    /// # Returns
    ///
    /// A new `PvMetadata` instance with the provided configuration and derived fields.
    pub fn discover_metadata_from_config(
        &self,
        key: &crate::models::phoebus::Key,
        config: &Config,
        topic: &str,
    ) -> PvMetadata {
        PvMetadata {
            config: config.clone(),
            display_path: key.display_path.clone(),
            phoebus_topic: Self::normalize_topic(topic),
        }
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
        self.pv_metadata
            .write()
            .await
            .insert(device.to_string(), new_metadata);
    }

    /// Normalizes a Phoebus topic name by stripping the "Command" suffix if present.
    ///
    /// This ensures consistent topic naming regardless of whether the original
    /// topic was a command topic or a config/state topic.
    ///
    /// # Arguments
    ///
    /// - `topic`: The topic name to normalize
    ///
    /// # Returns
    ///
    /// The normalized topic name (without "Command" suffix).
    fn normalize_topic(topic: &str) -> String {
        topic.strip_suffix("Command").unwrap_or(topic).to_owned()
    }
}

impl PartialEq for MetadataScope {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.pv_metadata, &other.pv_metadata)
    }
}

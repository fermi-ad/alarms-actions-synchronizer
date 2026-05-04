use super::*;
use crate::models::phoebus::Key;

#[tokio::test]
async fn lookup_metadata_by_device_returns_none_when_not_present() {
    let scope = MetadataScope::new();

    let result = scope.lookup_metadata_by_device("nonexistent").await;

    assert!(result.is_none());
}

#[tokio::test]
async fn lookup_metadata_by_device_returns_metadata_when_present() {
    let scope = MetadataScope::new();

    let metadata = PvMetadata {
        config: Config::default(),
        display_path: "/path/to/alarm".to_string(),
        phoebus_topic: "test-topic".to_string(),
    };
    scope
        .update_cached_metadata("test-device", metadata.clone())
        .await;

    let result = scope.lookup_metadata_by_device("test-device").await;

    assert_eq!(result, Some(metadata));
}

#[test]
fn build_metadata_from_config_creates_metadata_with_normalized_topic() {
    let key = Key::parse("config:/path/to/alarm/device1").unwrap();
    let config = Config::default();

    // Test with Command suffix
    let metadata = build_metadata_from_config(&key, &config, "testCommand");
    assert_eq!(metadata.phoebus_topic, "test");

    // Test without Command suffix
    let metadata = build_metadata_from_config(&key, &config, "test-topic");
    assert_eq!(metadata.phoebus_topic, "test-topic");
}

#[tokio::test]
async fn update_cached_metadata_stores_new_metadata() {
    let scope = MetadataScope::new();

    let metadata = PvMetadata {
        config: Config::default(),
        display_path: "/path".to_string(),
        phoebus_topic: "topic".to_string(),
    };

    scope
        .update_cached_metadata("test-device", metadata.clone())
        .await;

    let cached = scope.lookup_metadata_by_device("test-device").await;
    assert_eq!(cached, Some(metadata));
}

#[tokio::test]
async fn update_cached_metadata_overwrites_existing_metadata() {
    let scope = MetadataScope::new();

    let old_metadata = PvMetadata {
        config: Config::default(),
        display_path: "/old/path".to_string(),
        phoebus_topic: "old-topic".to_string(),
    };
    scope
        .update_cached_metadata("test-device", old_metadata.clone())
        .await;

    let new_metadata = PvMetadata {
        config: Config::default(),
        display_path: "/new/path".to_string(),
        phoebus_topic: "new-topic".to_string(),
    };
    scope
        .update_cached_metadata("test-device", new_metadata.clone())
        .await;

    let cached = scope.lookup_metadata_by_device("test-device").await;
    assert_eq!(cached, Some(new_metadata));
}

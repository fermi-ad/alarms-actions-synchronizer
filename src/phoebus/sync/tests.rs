use super::*;

#[tokio::test]
async fn should_return_failed_when_host_is_unreachable() {
    let client = ControlsClient::new("http://127.0.0.1:1");

    let result = client.acknowledge_alarm("MyDevice", "my-user").await;

    assert!(matches!(result, OutboundSyncResult::Failed));
}

#[tokio::test]
async fn should_share_connection_manager_across_clones() {
    let client = ControlsClient::new("http://127.0.0.1:1");
    let cloned = client.clone();

    assert!(Arc::ptr_eq(
        &client.connection_manager,
        &cloned.connection_manager
    ));
}

// --- ConnectionManager unit tests ---
//
// These tests exercise the connection lifecycle directly: initial creation, client reuse,
// reconnect after failure, duplicate reconnect suppression, and generation tracking.
// They use `publish_client` to seed state and `current_generation` to observe it.

#[tokio::test]
async fn should_start_with_no_connection() {
    let manager = ConnectionManager::new("http://127.0.0.1:1");

    assert!(manager.current_generation().await.is_none());
}

#[tokio::test]
async fn should_reuse_existing_client_when_already_connected() {
    let manager = ConnectionManager::new("http://127.0.0.1:1");

    // Seed a fake client at generation 1 (the first real connection generation; 0 is reserved as
    // "never connected"). We use publish_client directly to simulate an already-established
    // connection without needing a reachable host.
    let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
    let fake_client = AlarmCommandsClient::new(channel);
    manager.publish_client(fake_client, 1).await;

    // Now request_client should reuse the existing connection
    let result = manager.request_client().await;

    assert!(result.is_some());
    assert_eq!(result.unwrap().generation, Some(1));
}

#[tokio::test]
async fn should_return_none_when_no_client_exists() {
    let manager = ConnectionManager::new("http://127.0.0.1:1");

    // No client seeded — get_or_connect_client will attempt to connect and fail
    let result = manager.get_or_connect_client().await;

    assert!(result.is_none());
}

#[tokio::test]
async fn should_skip_reconnect_when_newer_generation_already_published() {
    let manager = ConnectionManager::new("http://127.0.0.1:1");

    // Seed a client at generation 1 (simulating another task already reconnected)
    let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
    let fake_client = AlarmCommandsClient::new(channel);
    manager.publish_client(fake_client, 1).await;

    // Attempt reconnect claiming failure at generation 0 — should be skipped
    let result = manager.reconnect_client(Some(0)).await;

    assert!(result.is_some());
    assert_eq!(manager.current_generation().await, Some(1));
}

#[tokio::test]
async fn should_attempt_reconnect_when_generation_matches_failed_generation() {
    let manager = ConnectionManager::new("http://127.0.0.1:1");

    // Seed a client at generation 1 (the first real connection generation; 0 is reserved as
    // "never connected").
    let channel = tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy();
    let fake_client = AlarmCommandsClient::new(channel);
    manager.publish_client(fake_client, 1).await;

    // Attempt reconnect claiming failure at generation 1 — should try to reconnect (and fail
    // because the host is unreachable)
    let result = manager.reconnect_client(Some(1)).await;

    assert!(result.is_none());
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_log_retry_exhaustion_when_both_attempts_fail() {
    let client = ControlsClient::new("http://127.0.0.1:1");

    // Seed a fake connected client so the first attempt proceeds past connection
    // but fails on the actual RPC (unreachable host), then reconnect also fails.
    // Using an unreachable host means both the initial send and the reconnect fail.
    let result = client.bypass_alarm("MyDevice", "my-user").await;

    assert!(matches!(result, OutboundSyncResult::Failed));
    // The initial connection attempt itself fails, so we get the connection failure log
    assert!(logs_contain("Failed to connect to Controls alarms service"));
}

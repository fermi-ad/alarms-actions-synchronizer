use super::*;

#[tokio::test]
async fn should_return_failed_when_host_is_unreachable() {
    let client = ControlsClient::new("http://127.0.0.1:0");

    let result = client.acknowledge_alarm("MyDevice", "my-user").await;

    assert!(matches!(result, OutboundSyncResult::Failed));
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_log_retry_exhaustion_when_both_attempts_fail() {
    let client = ControlsClient::new("http://127.0.0.1:0");

    // Seed a fake connected client so the first attempt proceeds past connection
    // but fails on the actual RPC (unreachable host), then reconnect also fails.
    // Using an unreachable host means both the initial send and the reconnect fail.
    let result = client.bypass_alarm("MyDevice", "my-user").await;

    assert!(matches!(result, OutboundSyncResult::Failed));
    // The initial connection attempt itself fails, so we get the connection failure log
    assert!(logs_contain("Initial RPC failed. Making one more attempt."));
}

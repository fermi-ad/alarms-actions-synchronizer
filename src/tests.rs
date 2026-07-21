//! The tests for the main.rs file.

use rust_pubsub_lib::{Message, MessageStream, PubSubError, Publisher, Snapshot, Subscriber};

use super::*;
use crate::models::Synchronizer;

#[derive(Debug)]
struct MockPubSub;
impl Publisher for MockPubSub {
    fn new(_: String, _: String) -> Self {
        unimplemented!()
    }

    async fn publish<M: Message>(&self, _: M) -> Result<(), PubSubError> {
        unimplemented!()
    }
}
impl Snapshot for MockPubSub {
    async fn get<M: Message>(_: String, _: String) -> Result<Vec<M>, PubSubError> {
        unimplemented!()
    }
}
impl Subscriber for MockPubSub {
    fn new(_: String, _: String) -> Self {
        unimplemented!()
    }

    async fn get_stream<M: Message + 'static>(&self) -> MessageStream<M> {
        Box::pin(tokio_stream::empty())
    }
}

struct MockSync;
impl Synchronizer<MockPubSub, MockPubSub> for MockSync {
    fn new(_: SynchronizerConfig) -> Self {
        MockSync
    }

    async fn synchronize<SNAP: Snapshot>(self) {
        // Do nothing
    }
}

impl RuntimeSyncFactory for MockSync {
    fn new(config: SynchronizerConfig) -> Self {
        <Self as Synchronizer<MockPubSub, MockPubSub>>::new(config)
    }

    async fn run(self) {
        <Self as Synchronizer<MockPubSub, MockPubSub>>::synchronize::<MockPubSub>(self).await
    }
}

#[test]
fn should_create_sync_config() {
    let result = create_synchronizer_config().unwrap();
    assert!(!result.controls_host.is_empty());
    assert!(!result.controls_topic.is_empty());
    assert!(!result.phoebus_host.is_empty());
    assert!(!result.phoebus_topics.is_empty());
}

#[test]
#[tracing_test::traced_test]
fn should_setup_logging_returns_err_when_already_initialized() {
    // tracing-test already sets a global default subscriber, so setup_logging() should
    // return Err(LoggingInitError::AlreadyInitialized) rather than panicking.
    use crate::models::LoggingInitError;
    let result = setup_logging();
    assert_eq!(result, Err(LoggingInitError::AlreadyInitialized));
}

#[tokio::test]
async fn should_begin_sync() {
    let handle = begin_sync::<MockSync>(create_synchronizer_config().unwrap());
    assert_eq!((), handle.await.unwrap());
}

#[tokio::test]
async fn should_spawn_health_server() {
    let token = CancellationToken::new();
    token.cancel();
    let result = spawn_health_server(token);
    assert!(result.is_ok());
}

//! The tests for the main.rs file.

use super::*;
use rust_pubsub_lib::{Message, PubSubError, StringMessage};
use tokio_stream::Stream;

#[derive(Debug)]
struct MockPubSub;
#[async_trait::async_trait]
impl Publisher for MockPubSub {
    fn new(_: String, _: String) -> Self {
        unimplemented!()
    }

    async fn publish<T, M: Message<T>>(&self, _: M) -> Result<(), PubSubError> {
        unimplemented!()
    }
}
#[async_trait::async_trait]
impl Snapshot for MockPubSub {
    async fn get<T, M: Message<T>>(_: String, _: String) -> Result<Vec<M>, PubSubError> {
        unimplemented!()
    }
}
#[async_trait::async_trait]
impl Subscriber for MockPubSub {
    fn new(_: String, _: String) -> Self {
        unimplemented!()
    }

    async fn get_stream<T, M: Message<T>>(
        &mut self,
    ) -> Result<impl Stream<Item = Result<M, PubSubError>>, PubSubError> {
        Ok(tokio_stream::empty())
    }
}

struct MockSync;
#[async_trait::async_trait]
impl Synchronizer<MockPubSub, MockPubSub> for MockSync {
    fn new(_: SynchronizerConfig) -> Self {
        MockSync
    }

    async fn synchronize<SNAP: Snapshot>(mut self) {
        // Do nothing
    }
}

#[test]
fn should_create_sync_config() {
    let result = create_synchronizer_config();
    assert!(!result.controls_host.is_empty());
    assert!(!result.controls_topic.is_empty());
    assert!(!result.phoebus_host.is_empty());
    assert!(!result.phoebus_topics.is_empty());
}

#[test]
#[should_panic]
#[tracing_test::traced_test]
fn should_setup_logging() {
    // Panics due to the tracing-test library already setting a global default
    setup_logging();
}

#[tokio::test]
async fn should_begin_sync() {
    let handle =
        begin_sync::<MockPubSub, MockPubSub, MockPubSub, MockSync>(create_synchronizer_config());
    assert_eq!((), handle.await.unwrap());
}

#[test]
#[should_panic]
fn new_mock_pubsub_as_publisher() {
    let _ = <MockPubSub as Publisher>::new(String::new(), String::new());
}

#[test]
#[should_panic]
fn new_mock_pubsub_as_subscriber() {
    let _ = <MockPubSub as Subscriber>::new(String::new(), String::new());
}

#[tokio::test]
#[should_panic]
async fn mock_pubsub_publish() {
    let _ = MockPubSub
        .publish(StringMessage::from_value(String::new()))
        .await;
}

#[tokio::test]
#[should_panic]
async fn mock_pubsub_snapshot() {
    let _ = MockPubSub::get::<String, StringMessage>(String::new(), String::new()).await;
}

#[tokio::test]
async fn mock_pubsub_stream() {
    assert!(
        MockPubSub
            .get_stream::<String, StringMessage>()
            .await
            .is_ok()
    );
}

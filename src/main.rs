//! Alarms Actions Synchronizer
//!
//! An app to synchronize the user actions between the Controls and Phoebus alarms servers.

use models::{Synchronizer, SynchronizerConfig};
use rust_pubsub_lib::{
    Publisher, Snapshot, Subscriber,
    kafka_impl::{KafkaPublisher, KafkaSnapshot, KafkaSubscriber},
};
use std::env;
use tokio::{
    task::{JoinError, JoinHandle},
    try_join,
};
use tracing_subscriber::{Registry, filter::EnvFilter, fmt::layer, layer::SubscriberExt};

mod controls;
mod models;
mod phoebus;
mod utils;

/// The entrypoint into the application, this method sets everything in motion.
#[tokio::main]
async fn main() -> Result<(), JoinError> {
    setup_logging();

    let sync_config = create_synchronizer_config();

    let phoebus_handle = begin_phoebus_sync(sync_config.clone());
    let controls_handle = begin_controls_sync(sync_config);
    try_join!(phoebus_handle, controls_handle).map(|_| ())
}

/// Generates an instance of [`SynchronizerConfig`] from the environment variables.
///
/// # Panics
/// Ends the process if any of the variables are not set.
fn create_synchronizer_config() -> SynchronizerConfig {
    let controls_host =
        env::var("CONTROLS_HOST").expect("CONTROLS_HOST environment variable not set");
    let controls_topic =
        env::var("CONTROLS_TOPIC").expect("CONTROLS_TOPIC environment variable not set");
    let phoebus_host = env::var("PHOEBUS_HOST").expect("PHOEBUS_HOST environment variable not set");
    let phoebus_topics: Vec<String> = env::var("PHOEBUS_TOPICS")
        .expect("PHOEBUS_TOPICS environment variable not set")
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    SynchronizerConfig::new(controls_host, controls_topic, phoebus_host, phoebus_topics)
}

/// Convenience method for kicking off the Controls-to-Phoebus synchronizer
fn begin_controls_sync(sync_config: SynchronizerConfig) -> JoinHandle<()> {
    begin_sync::<
        KafkaPublisher,
        KafkaSnapshot,
        KafkaSubscriber,
        controls::SyncImpl<KafkaPublisher, KafkaSubscriber>,
    >(sync_config)
}

/// Convenience method for kicking off the Phoebus-to-Controls synchronizer
fn begin_phoebus_sync(sync_config: SynchronizerConfig) -> JoinHandle<()> {
    begin_sync::<KafkaPublisher, KafkaSnapshot, KafkaSubscriber, phoebus::SyncImpl<KafkaSubscriber>>(
        sync_config,
    )
}

/// Spawns a new Tokio task containing a running instance of the configured [`Synchronizer`] type.
///
/// This allows the sync operations to run concurrently.
fn begin_sync<P: Publisher, SNAP: Snapshot, S: Subscriber, T: Synchronizer<P, S> + Send + Sync>(
    sync_config: SynchronizerConfig,
) -> JoinHandle<()> {
    tokio::spawn(async {
        let mut synchronizer = T::new(sync_config);
        synchronizer.synchronize::<SNAP>().await
    })
}

/// Initializes the logging framework.
///
/// # Panics
/// Ends the process if the logger fails to be set.
fn setup_logging() {
    let fmt_layer = layer()
        .with_target(false)
        .with_file(true)
        .with_line_number(true);
    // The following reads the log levels specified in the RUST_LOG environment variable. Allows us to configure logging
    // at both the application level and for specific crates/modules.
    let level_layer = EnvFilter::from_default_env();
    let subscriber = Registry::default().with(fmt_layer).with(level_layer);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set up logger");
}

/// The tests for this file.
#[cfg(test)]
mod test {
    use super::*;
    use crate::utils::testing::get_mock_sync_config;
    use rust_pubsub_lib::{Message, PubSubError};
    use tokio_stream::wrappers::BroadcastStream;

    #[derive(Debug)]
    struct MockPubSub;
    impl Publisher for MockPubSub {
        fn new(_: String, _: String) -> Self {
            unimplemented!()
        }

        fn publish(&mut self, _: Message) -> Result<(), PubSubError> {
            unimplemented!()
        }
    }
    impl Snapshot for MockPubSub {
        fn get(_: String, _: String) -> Result<Vec<Message>, PubSubError> {
            unimplemented!()
        }
    }
    impl Subscriber for MockPubSub {
        fn new(_: String, _: String) -> Self {
            unimplemented!()
        }

        fn get_stream(&self) -> BroadcastStream<Message> {
            unimplemented!()
        }
    }

    struct MockSync;
    #[async_trait::async_trait]
    impl Synchronizer<MockPubSub, MockPubSub> for MockSync {
        fn new(_: SynchronizerConfig) -> Self {
            MockSync
        }

        async fn synchronize<SNAP: Snapshot>(&mut self) {
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
            begin_sync::<MockPubSub, MockPubSub, MockPubSub, MockSync>(get_mock_sync_config());
        assert_eq!((), handle.await.unwrap());
    }
}

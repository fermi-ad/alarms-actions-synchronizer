//! Alarms Actions Synchronizer
//!
//! An app to synchronize the user actions between the Controls and Phoebus alarms servers.

use models::{Synchronizer, SynchronizerConfig};
use rust_env_var_lib::env_var;
use rust_pubsub_lib::{
    Publisher, Snapshot, Subscriber,
    kafka_impl::{KafkaPublisher, KafkaSnapshot, KafkaSubscriber},
};
use tokio::{
    signal,
    task::{JoinError, JoinHandle},
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{Registry, filter::EnvFilter, fmt::layer, layer::SubscriberExt};

mod controls;
mod models;
mod phoebus;
mod utils;

#[cfg(test)]
mod tests;

/// The entrypoint into the application, this method sets everything in motion.
#[tokio::main]
async fn main() -> Result<(), JoinError> {
    setup_logging();

    let sync_config = create_synchronizer_config();

    spawn_cancel_listener(sync_config.cancel_token.clone());

    let phoebus_handle = begin_phoebus_sync(sync_config.clone());
    let controls_handle = begin_controls_sync(sync_config);
    try_join!(phoebus_handle, controls_handle).map(|_| ())
}

/// Generates an instance of [`SynchronizerConfig`] from the environment variables.
///
/// # Panics
/// Ends the process if any of the variables are not set.
fn create_synchronizer_config() -> SynchronizerConfig {
    let controls_host = env_var::expect("CONTROLS_HOST");
    let controls_topic = env_var::expect("CONTROLS_TOPIC");
    let phoebus_host = env_var::expect("PHOEBUS_HOST");
    let phoebus_topics = env_var::expect::<String>("PHOEBUS_TOPICS")
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    SynchronizerConfig::new(
        CancellationToken::new(),
        controls_host,
        controls_topic,
        phoebus_host,
        phoebus_topics,
    )
}

fn spawn_cancel_listener(cancel_token: CancellationToken) {
    tokio::spawn(async move {
        signal::ctrl_c()
            .await
            .expect("Failed listening for Ctrl+C signal.");
        cancel_token.cancel();
    });
}

/// Convenience method for kicking off the Controls-to-Phoebus synchronizer
fn begin_controls_sync(sync_config: SynchronizerConfig) -> JoinHandle<()> {
    begin_sync::<KafkaPublisher, KafkaSnapshot, KafkaSubscriber, controls::SyncImpl<KafkaPublisher>>(
        sync_config,
    )
}

/// Convenience method for kicking off the Phoebus-to-Controls synchronizer
fn begin_phoebus_sync(sync_config: SynchronizerConfig) -> JoinHandle<()> {
    begin_sync::<KafkaPublisher, KafkaSnapshot, KafkaSubscriber, phoebus::SyncImpl>(sync_config)
}

/// Spawns a new Tokio task containing a running instance of the configured [`Synchronizer`] type.
///
/// This allows the sync operations to run concurrently.
fn begin_sync<P: Publisher, SNAP: Snapshot, S: Subscriber, T: Synchronizer<P, S> + Send + Sync>(
    sync_config: SynchronizerConfig,
) -> JoinHandle<()> {
    tokio::spawn(async {
        let synchronizer = T::new(sync_config);
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

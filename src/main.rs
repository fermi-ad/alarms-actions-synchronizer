//! Alarms Actions Synchronizer
//!
//! An app to synchronize the user actions between the Controls and Phoebus alarms servers.

use rust_env_var_lib::env_var;
use rust_pubsub_lib::KafkaPublisher;
use tokio::signal;
use tokio::task::{JoinError, JoinHandle};
use tokio::try_join;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tracing::{debug, error};
use tracing_subscriber::Registry;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::layer;
use tracing_subscriber::layer::SubscriberExt;

use models::{ConfigLoadError, LoggingInitError, RuntimeSyncFactory, SynchronizerConfig};

mod controls;
mod models;
mod phoebus;
mod utils;

#[cfg(test)]
mod tests;

const CONTROLS_HOST: &str = "CONTROLS_HOST";
const CONTROLS_TOPIC: &str = "CONTROLS_TOPIC";
const GRPC_ALARMS_SERVICE_HOST: &str = "GRPC_ALARMS_SERVICE_HOST";
const HEALTH_ADDR: &str = "HEALTH_ADDR";
const PHOEBUS_HOST: &str = "PHOEBUS_HOST";
const PHOEBUS_TOPICS: &str = "PHOEBUS_TOPICS";

/// The entrypoint into the application, this method sets everything in motion.
#[tokio::main]
async fn main() -> Result<(), JoinError> {
    setup_logging().expect("Failed to set up logger");

    let sync_config = create_synchronizer_config().expect("Failed to load configuration");

    spawn_health_server(sync_config.cancel_token.clone()).expect("Failed to spawn health endpoint");

    spawn_cancel_listener(sync_config.cancel_token.clone());

    let phoebus_handle = begin_phoebus_sync(sync_config.clone());
    let controls_handle = begin_controls_sync(sync_config);

    try_join!(phoebus_handle, controls_handle).map(|_| ())
}

/// Generates an instance of [`SynchronizerConfig`] from the environment variables.
///
/// Returns a [`ConfigLoadError`] if any required variable is not set.
fn create_synchronizer_config() -> Result<SynchronizerConfig, ConfigLoadError> {
    let controls_host: String = env_var::get(CONTROLS_HOST)
        .to_option()
        .ok_or_else(|| ConfigLoadError::MissingVariable(CONTROLS_HOST))?;
    let controls_topic: String = env_var::get(CONTROLS_TOPIC)
        .to_option()
        .ok_or_else(|| ConfigLoadError::MissingVariable(CONTROLS_TOPIC))?;
    let grpc_alarms_svc_host: String = env_var::get(GRPC_ALARMS_SERVICE_HOST)
        .to_option()
        .ok_or_else(|| ConfigLoadError::MissingVariable(GRPC_ALARMS_SERVICE_HOST))?;
    let phoebus_host: String = env_var::get(PHOEBUS_HOST)
        .to_option()
        .ok_or_else(|| ConfigLoadError::MissingVariable(PHOEBUS_HOST))?;
    let phoebus_topics: Vec<String> = env_var::get(PHOEBUS_TOPICS)
        .to_option::<String>()
        .ok_or_else(|| ConfigLoadError::MissingVariable(PHOEBUS_TOPICS))?
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    Ok(SynchronizerConfig::new(
        CancellationToken::new(),
        controls_host,
        controls_topic,
        grpc_alarms_svc_host,
        phoebus_host,
        phoebus_topics,
    ))
}

fn spawn_health_server(cancel_token: CancellationToken) -> Result<(), ConfigLoadError> {
    let health_addr = env_var::get(HEALTH_ADDR)
        .to_option()
        .ok_or_else(|| ConfigLoadError::MissingVariable(HEALTH_ADDR))?;

    tokio::spawn(async move {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_service_status("", ServingStatus::Serving)
            .await;

        debug!("Starting internal gRPC health server at {}", health_addr);

        tokio::select! {
            _ = cancel_token.cancelled() => {
                health_reporter
                    .set_service_status("", ServingStatus::NotServing)
                    .await;
            }
            server_exit = Server::builder()
                .add_service(health_service)
                .serve(health_addr) => {
                if let Err(err) = server_exit {
                    error!("{err}");
                    cancel_token.cancel();
                }
            }
        }
    });

    Ok(())
}

fn spawn_cancel_listener(cancel_token: CancellationToken) {
    tokio::spawn(async move {
        signal::ctrl_c()
            .await
            .expect("Failed listening for Ctrl+C signal.");
        cancel_token.cancel();
    });
}

/// Convenience method for kicking off the Controls-to-Phoebus synchronizer.
fn begin_controls_sync(sync_config: SynchronizerConfig) -> JoinHandle<()> {
    begin_sync::<controls::SyncImpl<KafkaPublisher>>(sync_config)
}

/// Convenience method for kicking off the Phoebus-to-Controls synchronizer.
fn begin_phoebus_sync(sync_config: SynchronizerConfig) -> JoinHandle<()> {
    begin_sync::<phoebus::SyncImpl>(sync_config)
}

/// Spawns a new Tokio task containing a running instance of the configured runtime synchronizer type.
///
/// This keeps the abstraction surface focused on the concrete Kafka runtime used by the application while
/// still allowing tests to instantiate synchronizers directly through [`Synchronizer`].
fn begin_sync<T>(sync_config: SynchronizerConfig) -> JoinHandle<()>
where
    T: RuntimeSyncFactory + Send + 'static,
{
    tokio::spawn(async move {
        let synchronizer = T::new(sync_config);
        synchronizer.run().await
    })
}

/// Initializes the logging framework.
///
/// Returns a [`LoggingInitError`] if a global subscriber has already been set.
fn setup_logging() -> Result<(), LoggingInitError> {
    let fmt_layer = layer()
        .with_target(false)
        .with_file(true)
        .with_line_number(true);
    // The following reads the log levels specified in the RUST_LOG environment variable. Allows us to configure logging
    // at both the application level and for specific crates/modules.
    let level_layer = EnvFilter::from_default_env();
    let subscriber = Registry::default().with(fmt_layer).with(level_layer);
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_| LoggingInitError::AlreadyInitialized)
}

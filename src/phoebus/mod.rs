//! Phoebus Module
//!
//! Contains the [`Synchronizer`] for pushing Phoebus commands and configs into the Controls alarm server.

use rust_pubsub_lib::{
    KafkaPublisher, KafkaSnapshot, KafkaSubscriber, Publisher, Snapshot, Subscriber,
};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::models::phoebus::KeyParseError;
use crate::models::{
    IgnoreReason, RuntimeSyncFactory, SkipReason, SyncOutcome, Synchronizer, SynchronizerConfig,
};
use crate::phoebus::monitor::Monitor;
use crate::utils::get_command_topic;
use init::get_existing_messages_from_phoebus;
use sync::ControlsClient;

mod init;
mod monitor;
mod sync;

#[cfg(test)]
mod tests;

/// Implementation of [`Synchronizer`] for pushing Phoebus commands and configs into the Controls alarm service.
pub struct SyncImpl {
    config: SynchronizerConfig,
}

#[async_trait::async_trait]
impl<P: Publisher, S: Subscriber + Send + Sync + 'static> Synchronizer<P, S> for SyncImpl {
    fn new(config: SynchronizerConfig) -> Self {
        SyncImpl { config }
    }

    async fn synchronize<SNAP: Snapshot>(self) {
        info!("Starting Phoebus-to-Controls Synchronizer");
        tokio::select! {
            _ = self.config.cancel_token.cancelled() => return,
            _ = get_existing_messages_from_phoebus::<SNAP>(
                self.config.phoebus_host.clone(),
                self.config.phoebus_topics.clone(),
                &self.config.alarm_states,
                &self.config.metadata_scope,
            ) => {}
        }

        let controls_client = ControlsClient::new(&self.config.grpc_alarms_svc_host);
        let with_command_topics = self
            .config
            .phoebus_topics
            .iter()
            .flat_map(|topic| [topic.clone(), get_command_topic(topic)])
            .collect::<Vec<_>>();
        let mut monitors = JoinSet::new();
        for topic in with_command_topics {
            let monitor = Monitor::new(topic, &self.config, controls_client.clone());
            monitors.spawn(monitor.start::<S>());
        }

        tokio::select! {
            _ = self.config.cancel_token.cancelled() => {}
            _ = monitors.join_all() => {}
        }
    }
}

#[async_trait::async_trait]
impl RuntimeSyncFactory for SyncImpl {
    fn new(config: SynchronizerConfig) -> Self {
        // Use the fully-qualified syntax to disambiguate which `new` method we're calling
        <Self as Synchronizer<KafkaPublisher, KafkaSubscriber>>::new(config)
    }

    async fn run(self) {
        // Use the fully-qualified syntax to satisfy the compiler's check that we're making a call on an instance of `Synchronizer`
        <Self as Synchronizer<KafkaPublisher, KafkaSubscriber>>::synchronize::<KafkaSnapshot>(self)
            .await
    }
}

fn map_key_parse_error(
    context: &str,
    key: &str,
    value: &str,
    error: &KeyParseError,
) -> SyncOutcome {
    match error {
        KeyParseError::UnsupportedOperation => {
            debug!(
                context = context,
                "Ignoring Phoebus message because its key uses an untracked operation prefix.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Ignored {
                reason: IgnoreReason::StateNoise,
            }
        }
        KeyParseError::MalformedStructure => {
            warn!(
                context = context,
                "Skipping malformed Phoebus key: expected '<operation>:<display path>/<device>'.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Skipped {
                reason: SkipReason::MalformedMessage,
            }
        }
        KeyParseError::EmptyDevice => {
            warn!(
                context = context,
                "Skipping Phoebus key with empty device name. Empty device names are treated as invalid.\n Original message from Phoebus: {{ key: {key}, text: {value} }}"
            );
            SyncOutcome::Skipped {
                reason: SkipReason::MalformedMessage,
            }
        }
    }
}

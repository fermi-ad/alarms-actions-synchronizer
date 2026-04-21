//! Phoebus Module
//!
//! Contains the [`Synchronizer`] for pushing Phoebus commands and configs into the Controls alarm server.

use crate::models::{Synchronizer, SynchronizerConfig};
use crate::phoebus::monitor::Monitor;
use crate::utils::get_command_topic;
use init::get_existing_messages_from_phoebus;
use rust_pubsub_lib::{Publisher, Snapshot, Subscriber};
use sync::ControlsClient;
use tokio::task::JoinSet;
use tracing::info;

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
                &self.config.pv_metadata,
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

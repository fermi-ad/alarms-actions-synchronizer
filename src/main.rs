use std::{collections::HashMap, env, sync::Arc};

use tokio::{sync::RwLock, task::JoinHandle};
use tracing_subscriber::{Registry, filter::EnvFilter, fmt::layer, layer::SubscriberExt};
mod controls;
mod models;
use models::{CachedState, phoebus::PvMetadata};
mod phoebus;
mod utils;

type AlarmStateCache = Arc<RwLock<HashMap<String, CachedState>>>;
type PvCache = Arc<RwLock<HashMap<String, PvMetadata>>>;

#[tokio::main]
async fn main() {
    setup_logging();

    let sync_config = create_synchronizer_config();

    let phoebus_handle = begin_sync::<phoebus::SyncImpl>(sync_config.clone());
    let controls_handle = begin_sync::<controls::SyncImpl>(sync_config);
    let _ = controls_handle.await;
    let _ = phoebus_handle.await;
}

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
    SynchronizerConfig {
        alarm_states: Arc::new(RwLock::new(HashMap::<String, CachedState>::new())),
        controls_host,
        controls_topic,
        phoebus_host,
        phoebus_topics,
        pv_metadata: Arc::new(RwLock::new(HashMap::<String, PvMetadata>::new())),
    }
}

fn begin_sync<T: Synchronizer + Send + Sync>(sync_config: SynchronizerConfig) -> JoinHandle<()> {
    tokio::spawn(async {
        let mut synchronizer = T::new(sync_config);
        synchronizer.synchronize().await
    })
}

struct SynchronizerConfig {
    alarm_states: AlarmStateCache,
    controls_host: String,
    controls_topic: String,
    phoebus_host: String,
    phoebus_topics: Vec<String>,
    pv_metadata: PvCache,
}
impl Clone for SynchronizerConfig {
    fn clone(&self) -> Self {
        SynchronizerConfig {
            alarm_states: Arc::clone(&self.alarm_states),
            controls_host: self.controls_host.clone(),
            controls_topic: self.controls_topic.clone(),
            phoebus_host: self.phoebus_host.clone(),
            phoebus_topics: self.phoebus_topics.clone(),
            pv_metadata: Arc::clone(&self.pv_metadata),
        }
    }
}

#[async_trait::async_trait]
trait Synchronizer {
    fn new(config: SynchronizerConfig) -> Self;
    async fn synchronize(&mut self);
}

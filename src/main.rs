use tokio::task::JoinHandle;
use tracing_subscriber::{Registry, filter::EnvFilter, fmt::layer, layer::SubscriberExt};
mod controls;
mod generated;
mod phoebus;
mod util;

#[tokio::main]
async fn main() {
    setup_logging();

    let controls_handle = begin_sync::<controls::SyncImpl>();
    let phoebus_handle = begin_sync::<phoebus::SyncImpl>();

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

fn begin_sync<T: Synchronizer + Send + Sync>() -> JoinHandle<()> {
    tokio::spawn(async {
        let mut synchronizer = T::new();
        synchronizer.synchronize().await
    })
}

#[async_trait::async_trait]
trait Synchronizer {
    fn new() -> Self;
    async fn synchronize(&mut self);
}

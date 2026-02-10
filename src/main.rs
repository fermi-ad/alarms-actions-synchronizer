mod controls;
mod phoebus;

#[tokio::main]
async fn main() {
    setup_logging();

    let controls_handle = tokio::spawn(async {
        let mut synchronizer = controls::SyncImpl::new();
        synchronizer.synchronize().await
    });
    let phoebus_handle = tokio::spawn(async {
        let mut synchronizer = phoebus::SyncImpl::new();
        synchronizer.synchronize().await
    });

    let _ = controls_handle.await;
    let _ = phoebus_handle.await;
}

fn setup_logging() {
    let subscriber = tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set logger");
}

#[async_trait::async_trait]
trait Synchronizer {
    fn new() -> Self;
    async fn synchronize(&mut self);
}

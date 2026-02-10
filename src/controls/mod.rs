use super::Synchronizer;
use rust_pubsub_lib::{
    Publisher, Subscriber,
    kafka_impl::{KafkaPublisher, KafkaSubscriber},
};
use std::env;
use tokio_stream::StreamExt;
use tracing::warn;

pub struct SyncImpl {
    controls: KafkaSubscriber,
    phoebus: Vec<KafkaPublisher>,
}

#[async_trait::async_trait]
impl Synchronizer for SyncImpl {
    fn new() -> Self {
        let controls_host =
            env::var("CONTROLS_HOST").expect("CONTROLS_HOST environment variable not set");
        let controls_topic =
            env::var("CONTROLS_TOPIC").expect("CONTROLS_TOPIC environment variable not set");
        let phoebus_host =
            env::var("PHOEBUS_HOST").expect("PHOEBUS_HOST environment variable not set");
        let phoebus_topics: Vec<String> = env::var("PHOEBUS_TOPICS")
            .expect("PHOEBUS_TOPICS environment variable not set")
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        SyncImpl {
            controls: KafkaSubscriber::new(controls_host, controls_topic),
            phoebus: phoebus_topics
                .into_iter()
                .map(|topic| KafkaPublisher::new(phoebus_host.clone(), topic))
                .collect(),
        }
    }

    async fn synchronize(&mut self) {
        let mut controls_stream = self.controls.get_stream();
        loop {
            let msg_opt = controls_stream.next().await;
            if msg_opt.is_none() {
                controls_stream = self.controls.get_stream();
                continue;
            }
            match msg_opt.unwrap() {
                Ok(msg) => {
                    for publisher in &mut self.phoebus {
                        if let Err(err) = publisher.publish(msg.clone()) {
                            warn!("Failed publishing to Phoebus Kafka.\n  Cause: {err:?}");
                        }
                    }
                }
                Err(e) => warn!("Error receiving message from Controls Kafka.\n  Cause: {e:?}"),
            }
        }
    }
}

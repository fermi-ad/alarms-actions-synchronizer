use super::Synchronizer;
use rust_pubsub_lib::{
    Message, Publisher, Subscriber,
    kafka_impl::{KafkaPublisher, KafkaSubscriber},
};
use std::env;
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose, wrappers::BroadcastStream};
use tracing::warn;

pub struct SyncImpl {
    controls: KafkaPublisher,
    phoebus: Vec<KafkaSubscriber>,
}
impl SyncImpl {
    fn generate_stream_map(
        &mut self,
    ) -> StreamMap<usize, StreamNotifyClose<BroadcastStream<Message>>> {
        let streams = self
            .phoebus
            .iter_mut()
            .map(|sub| sub.get_stream())
            .collect::<Vec<_>>();
        let mut stream_map = StreamMap::new();
        for (idx, stream) in streams.into_iter().enumerate() {
            stream_map.insert(idx, StreamNotifyClose::new(stream));
        }
        stream_map
    }
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
            controls: KafkaPublisher::new(controls_host, controls_topic),
            phoebus: phoebus_topics
                .into_iter()
                .map(|topic| KafkaSubscriber::new(phoebus_host.clone(), topic))
                .collect(),
        }
    }

    async fn synchronize(&mut self) {
        let mut stream_map = self.generate_stream_map();
        loop {
            let stream_opt = stream_map.next().await;
            if stream_opt.is_none() {
                // All of the streams closed themselves. Regenerate the map.
                stream_map = self.generate_stream_map();
                continue;
            }
            let (idx, msg_opt) = stream_opt.unwrap();
            if msg_opt.is_none() {
                // One of the streams closed itself. Regenerate the stream.
                stream_map.insert(
                    idx,
                    StreamNotifyClose::new(self.phoebus.get(idx).unwrap().get_stream()),
                );
                continue;
            }
            let msg_result = msg_opt.unwrap();
            match msg_result {
                Ok(msg) => match self.controls.publish(msg) {
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            "Publisher failed connecting to Controls Kafka. Attempting to reconnect.\n  Cause: {e:?}"
                        );
                    }
                },
                Err(e) => {
                    warn!(
                        "Consumer lost connection to Phoebus Kafka. Attempting to reconnect.\n  Cause: {e:?}"
                    );
                }
            }
        }
    }
}

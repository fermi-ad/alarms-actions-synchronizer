use super::{AlarmStateCache, PvCache, Synchronizer, SynchronizerConfig};
use crate::{models::phoebus::PvMetadata, utils::get_command_topic};
use rust_pubsub_lib::{
    Message, Publisher, Subscriber,
    kafka_impl::{KafkaPublisher, KafkaSubscriber},
};
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose, wrappers::BroadcastStream};
use tracing::warn;

pub struct SyncImpl {
    alarm_states: AlarmStateCache,
    controls: KafkaPublisher,
    phoebus: Vec<KafkaSubscriber>,
    pv_metadata: PvCache,
}
impl SyncImpl {
    fn generate_stream_map(
        &mut self,
    ) -> StreamMap<usize, StreamNotifyClose<BroadcastStream<Message>>> {
        self.phoebus
            .iter()
            .map(|sub| StreamNotifyClose::new(sub.get_stream()))
            .enumerate()
            .collect::<StreamMap<_, _>>()
    }

    async fn process_message(&mut self, msg: Message) {
        todo!()
    }
}
#[async_trait::async_trait]
impl Synchronizer for SyncImpl {
    fn new(config: SynchronizerConfig) -> Self {
        SyncImpl {
            alarm_states: config.alarm_states,
            controls: KafkaPublisher::new(config.controls_host, config.controls_topic),
            phoebus: config
                .phoebus_topics
                .into_iter()
                .flat_map(|topic| {
                    [
                        KafkaSubscriber::new(
                            config.phoebus_host.clone(),
                            get_command_topic(&topic),
                        ),
                        KafkaSubscriber::new(config.phoebus_host.clone(), topic),
                    ]
                })
                .collect(),
            pv_metadata: config.pv_metadata,
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

            match msg_opt.unwrap() {
                Ok(msg) => {
                    self.process_message(msg).await;
                }
                Err(e) => {
                    warn!(
                        "Error with the internal Kafka Consumer stream. Reconnect in progress.\n Cause: {e:?}"
                    );
                }
            }
        }
    }
}

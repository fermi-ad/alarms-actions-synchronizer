//! Utilities Module
//!
//! Provides utility functions used throughout the application.

/// Generates the name of the command topic associated with the provided base topic.
///
/// Essentially, appends "Command" to the end of the topic name.
///
/// Returns a new instance, leaving the old reference intact.
pub fn get_command_topic(topic: &str) -> String {
    format!("{topic}Command")
}

#[cfg(test)]
mod test {
    //! The tests for this module.
    use super::*;

    #[test]
    fn should_get_command_topic() {
        assert_eq!("topicCommand", get_command_topic("topic"));
    }
}

#[cfg(test)]
pub mod testing {
    //! Utilities for use in other tests within this application.

    use crate::models::{Synchronizer, SynchronizerConfig};
    use rust_pubsub_lib::{Message, PubSubError, Publisher, Subscriber};
    use std::time::Duration;
    use tokio::{
        sync::broadcast::{Receiver, Sender, channel},
        time::sleep,
    };
    use tokio_stream::wrappers::BroadcastStream;

    #[derive(Debug)]
    pub struct TestPublisher {
        sender: Sender<Message>,
        _channel_lock: Receiver<Message>,
    }
    impl TestPublisher {
        pub fn get_receiver(&self) -> Receiver<Message> {
            self.sender.subscribe()
        }
    }
    impl Publisher for TestPublisher {
        fn new(_: String, _: String) -> Self {
            let (sender, receiver) = channel(10);
            TestPublisher {
                sender,
                _channel_lock: receiver,
            }
        }

        fn publish(&mut self, message: Message) -> Result<(), PubSubError> {
            let _ = self.sender.send(message);
            Ok(())
        }
    }

    #[derive(Debug)]
    pub struct TestSubscriber {
        pub sender: Sender<Message>,
        _receiver: Receiver<Message>,
    }
    impl Clone for TestSubscriber {
        fn clone(&self) -> Self {
            TestSubscriber::new(String::new(), String::new())
        }
    }
    impl Subscriber for TestSubscriber {
        fn new(_: String, _: String) -> Self {
            let (sender, _receiver) = channel(10);
            TestSubscriber { sender, _receiver }
        }

        fn get_stream(&self) -> BroadcastStream<Message> {
            BroadcastStream::new(self.sender.subscribe())
        }
    }

    pub fn get_mock_sync_config() -> SynchronizerConfig {
        SynchronizerConfig::new(
            String::new(),
            String::from("testTopic"),
            String::new(),
            vec![String::from("testTopic")],
        )
    }

    pub struct TestInstance<T>
    where
        T: Synchronizer<TestPublisher, TestSubscriber>,
    {
        message: Option<Message>,
        sender: Option<Sender<Message>>,
        sync: T,
    }
    impl<T: Synchronizer<TestPublisher, TestSubscriber> + Send + Sync + 'static> TestInstance<T> {
        pub fn check_that(sync: T) -> Self {
            TestInstance {
                message: None,
                sender: None,
                sync,
            }
        }

        pub fn when(mut self, sender: Sender<Message>, message: Message) -> Self {
            self.message = Some(message);
            self.sender = Some(sender);
            self
        }

        pub async fn satisfies(self, condition: impl AsyncFnMut() -> bool) -> Result<(), ()> {
            do_test(
                self.sync,
                self.sender.unwrap(),
                self.message.unwrap(),
                condition,
            )
            .await
        }
    }

    async fn do_test<T: Synchronizer<TestPublisher, TestSubscriber> + Send + Sync + 'static>(
        mut sync: T,
        sender: Sender<Message>,
        message: Message,
        condition: impl AsyncFnMut() -> bool,
    ) -> Result<(), ()> {
        // Asynchronously kick off the synchronizer in a separate task
        let handle = tokio::spawn(async move {
            sync.synchronize().await;
        });

        send_message_when_sync_starts(&sender, message).await;

        let result = wait_for_condition(condition).await;

        // Stop the synchronizer
        handle.abort();

        // Check that the desired condition was met
        if result { Ok(()) } else { Err(()) }
    }

    async fn send_message_when_sync_starts(sender: &Sender<Message>, message: Message) {
        for _ in 0..10 {
            sleep(Duration::from_millis(100)).await;
            if sender.receiver_count() > 1 {
                let _ = sender.send(message);
                return;
            }
        }
        panic!("The sync service did not start");
    }

    async fn wait_for_condition(mut condition: impl AsyncFnMut() -> bool) -> bool {
        for _ in 0..10 {
            if condition().await {
                return true;
            }
            sleep(Duration::from_millis(100)).await;
        }
        return false;
    }
}

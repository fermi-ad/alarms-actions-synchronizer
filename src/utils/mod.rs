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
    use rust_pubsub_lib::{Message, PubSubError, Publisher, Snapshot, Subscriber};
    use std::{error::Error, time::Duration};
    use tokio::{
        sync::broadcast::{Receiver, Sender, channel},
        time::{sleep, timeout},
    };
    use tokio_stream::wrappers::BroadcastStream;

    /// Implementation of [`Snapshot`] to use in tests.
    ///
    /// Always returns an empty vector. Used to short-circuit the initialization logic for the Phoebus synchronizer.
    #[derive(Debug)]
    struct TestSnapshot;
    impl Snapshot for TestSnapshot {
        fn get(_: String, _: String) -> Result<Vec<Message>, PubSubError> {
            Ok(Vec::new())
        }
    }

    /// Implementation of [`Publisher`] with a method to get a stream of the messages that are "published" by this object.
    #[derive(Debug)]
    pub struct TestPublisher {
        sender: Sender<Message>,
        _channel_lock: Receiver<Message>,
    }
    impl TestPublisher {
        /// Generates a [`Receiver`] that is subscribed to the sent messages of this `Publisher`.
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

    /// Implementation of [`Subscriber`] that exposes its [`Sender`] so that test cases may manually send messages.
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

    /// Generates a [`SynchronizerConfig`] instance for use in test cases. Simulates both Controls and Phoebus using a topic called "testTopic".
    pub fn get_mock_sync_config() -> SynchronizerConfig {
        SynchronizerConfig::new(
            String::new(),
            String::from("testTopic"),
            String::new(),
            vec![String::from("testTopic")],
        )
    }

    /// A helper tool that simulates a [`Synchronizer`] receiving the provided [`Message`] and checks that the expected behavior ensues.
    pub struct TestInstance<T>
    where
        T: Synchronizer<TestPublisher, TestSubscriber>,
    {
        message: Option<Message>,
        sender: Option<Sender<Message>>,
        sync: T,
    }
    impl<T: Synchronizer<TestPublisher, TestSubscriber> + Send + Sync + 'static> TestInstance<T> {
        /// Starts the test case by generating an instance of [`TestInstance`] for the provided [`Synchronizer`].
        pub fn check_that(sync: T) -> Self {
            TestInstance {
                message: None,
                sender: None,
                sync,
            }
        }

        /// Provides the [`Sender`] and [`Message`] that should be used to kick off the behavior being tested in the [`Synchronizer`].
        pub fn when(mut self, sender: Sender<Message>, message: Message) -> Self {
            self.message = Some(message);
            self.sender = Some(sender);
            self
        }

        /// Sends the configured [`Message`] and checks whether the provided [`condition`](AsyncFnMut) is met.
        pub async fn satisfies(
            self,
            condition: impl AsyncFnMut() -> bool,
        ) -> Result<(), Box<dyn Error>> {
            do_test(
                self.sync,
                self.sender.unwrap(),
                self.message.unwrap(),
                condition,
            )
            .await
        }
    }

    /// Helper method that starts the synchronizer, waits for the synchronizer's initialization to complete, sends the message
    /// to induce the behavior being tested, and checks to see whether the desired outcome was observed.
    /// Also gracefully handles the cleanup of the synchronizer task, so we don't leave a bunch of them running.
    async fn do_test<T: Synchronizer<TestPublisher, TestSubscriber> + Send + Sync + 'static>(
        mut sync: T,
        sender: Sender<Message>,
        message: Message,
        condition: impl AsyncFnMut() -> bool,
    ) -> Result<(), Box<dyn Error>> {
        // Asynchronously kick off the synchronizer in a separate task
        let handle = tokio::spawn(async move {
            sync.synchronize::<TestSnapshot>().await;
        });

        wait_for_sync_to_start(&sender).await;
        let _ = sender.send(message);

        let result = wait_for_condition(condition).await;

        // Stop the synchronizer
        handle.abort();

        result
    }

    /// Loops indefinitely while checking to see if a new reciever has subscribed to the provided [`Sender`].
    /// This indicates the [`Synchronizer`] has started receiving messages.
    async fn wait_for_new_receiver(sender: &Sender<Message>) {
        loop {
            sleep(Duration::from_millis(100)).await;
            if sender.receiver_count() > 1 {
                break;
            }
        }
    }

    /// Invokes [`wait_for_new_receiver`] inside of a [`timeout`] to fail the test if it takes more than 1 second
    /// for the [`Synchronizer`] to start.
    async fn wait_for_sync_to_start(sender: &Sender<Message>) {
        timeout(Duration::from_secs(1), wait_for_new_receiver(sender))
            .await
            .unwrap()
    }

    /// Loops indefinitely while checking to see if the provided [`condition`](AsyncFnMut) has been met.
    async fn try_condition(mut condition: impl AsyncFnMut() -> bool) {
        loop {
            if condition().await {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wraps [`try_condition`] in a [`timeout`] to fail the test if the [`condition`](AsyncFnMut) is not met within 1 second.
    async fn wait_for_condition(
        condition: impl AsyncFnMut() -> bool,
    ) -> Result<(), Box<dyn Error>> {
        Ok(timeout(Duration::from_secs(1), try_condition(condition)).await?)
    }
}

//! Test Runner Module
//!
//! Contains functions and constants centered around the [`TestRunner`] struct.
//! [`TestRunner`] descibes the common flow of a test case: some data appears on a given
//! Kafka topic and the [`Synchronizer`] under test is expected to respond in a certain way.

use super::*;
use crate::models::{Synchronizer, SynchronizerConfig};
use rust_pubsub_lib::{
    Message, Publisher,
    kafka_impl::{KafkaPublisher, KafkaSnapshot, KafkaSubscriber, testing_utils::Harness},
};
use std::{error::Error, time::Duration};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

/// The topic used in the tests for messages from Controls.
pub const CONTROLS_TOPIC: &str = "controls";

/// The topic used in the tests for messages to/from Phoebus.
pub const PHOEBUS_TOPIC: &str = "phoebus";

/// Describes the origin of the [`Message`] that initiates the behavior being tested.
///
/// Used by [`TestRunner`] to choose which topic from the [`SynchronizerConfig`] to send the message on.
pub enum MessageOrigin {
    Controls,
    Phoebus,
    PhoebusCommand,
}

/// Generates a [`SynchronizerConfig`] instance where the Kafka topics are the default testing
/// [`CONTROLS_TOPIC`] and [`PHOEBUS_TOPIC`].
pub fn get_mock_sync_config() -> SynchronizerConfig {
    SynchronizerConfig::new(
        CancellationToken::new(),
        String::new(),
        String::from(CONTROLS_TOPIC),
        String::new(),
        vec![String::from(PHOEBUS_TOPIC)],
    )
}

/// Generates a [`SynchronizerConfig`] instance where the Kafka topics are the default testing
/// [`CONTROLS_TOPIC`] and [`PHOEBUS_TOPIC`], but salted with a random hash value.
///
/// This ensures the test is run against empty topics, which is useful when specific edge cases are being tested.
pub fn get_mock_sync_config_salted() -> SynchronizerConfig {
    let salt = Uuid::new_v4().as_simple().to_string();
    SynchronizerConfig {
        controls_topic: format!("{CONTROLS_TOPIC}{salt}"),
        phoebus_topics: vec![format!("{PHOEBUS_TOPIC}{salt}")],
        ..get_mock_sync_config()
    }
}

/// A helper tool that simulates a [`Synchronizer`] receiving the provided [`Message`] and checks that the expected behavior ensues.
pub struct TestRunner<T>
where
    T: Synchronizer<KafkaPublisher, KafkaSubscriber> + Send + Sync + 'static,
{
    cancel_token: CancellationToken,
    /// The [`Harness`] containing the location of the test Kafka Cluster via its [`host()`](Harness::host) method.
    pub harness: Harness,
    message: Option<Message>,
    send_topic: String,
    /// The [`Synchronizer`] instance being tested.
    pub sync: T,
}
impl<T: Synchronizer<KafkaPublisher, KafkaSubscriber> + Send + Sync + 'static> TestRunner<T> {
    /// Generates a [`TestInstance`] for a [`Synchronizer`] that listens to messages from [`MessageOrigin`].
    ///
    /// A [`Message`] is simulated to be sent on the appropriate topic by calling [`has`](Self::has).
    ///
    /// The results of the test are then determined by calling one of
    /// - [`results_in`](Self::results_in)
    /// - [`after_init_results_in`](Self::after_init_results_in)
    /// - [`on_init_results_in`](Self::on_init_results_in)
    pub async fn check_when(origin: MessageOrigin, config_opt: Option<SynchronizerConfig>) -> Self {
        let mut config = config_opt.unwrap_or_else(get_mock_sync_config);

        let cancel_token = config.cancel_token.clone();

        let (send_topic, all_topics) = prioritize_topics(origin, &config);

        let harness = Harness::with_topics(all_topics).await;

        config.controls_host = harness.host();
        config.phoebus_host = harness.host();
        let sync = T::new(config);

        TestRunner {
            cancel_token,
            harness,
            message: None,
            send_topic,
            sync,
        }
    }

    /// Supplies the [`Message`] to send that kicks off the test case.
    pub fn has(mut self, message: Message) -> Self {
        self.message = Some(message);
        self
    }

    /// Tests the provided [`condition`](AsyncFnMut) occurs without regard to any initialization of the [`Synchronizer`] under test.
    pub async fn results_in(
        self,
        condition: impl AsyncFnMut() -> bool,
    ) -> Result<(), Box<dyn Error>> {
        // Asynchronously kick off the synchronizer in a separate task
        tokio::spawn(self.sync.synchronize::<KafkaSnapshot>());

        let result = do_test(
            self.harness,
            self.message.unwrap(),
            self.send_topic,
            condition,
        )
        .await;

        // Stop the synchronizer
        self.cancel_token.cancel();

        result
    }

    /// Tests the provided [`condition`](AsyncFnMut) occurs only after the [`Synchronizer`] under test is initialized.
    pub async fn after_init_results_in(
        self,
        check_for_initialization: impl AsyncFn() -> bool,
        condition: impl AsyncFnMut() -> bool,
    ) -> Result<(), Box<dyn Error>> {
        // Asynchronously kick off the synchronizer in a separate task
        tokio::spawn(self.sync.synchronize::<KafkaSnapshot>());

        if let Err(e) = wait_for_condition(check_for_initialization).await {
            // Stop the synchronizer
            self.cancel_token.cancel();
            return Err(e);
        }

        let result = do_test(
            self.harness,
            self.message.unwrap(),
            self.send_topic,
            condition,
        )
        .await;

        // Stop the synchronizer
        self.cancel_token.cancel();

        result
    }

    /// Tests the provided [`condition`](AsyncFnMut) occurs during the intitialization of the [`Synchronizer`] under test.
    pub async fn on_init_results_in(
        self,
        condition: impl AsyncFnMut() -> bool,
    ) -> Result<(), Box<dyn Error>> {
        send_test_message(&self.harness, self.message.unwrap(), self.send_topic).await?;

        // Asynchronously kick off the synchronizer in a separate task
        tokio::spawn(self.sync.synchronize::<KafkaSnapshot>());

        let result = wait_for_condition(condition).await;

        // Stop the synchronizer
        self.cancel_token.cancel();

        result
    }
}

/// Orders the topics from [`SynchronizerConfig`] according to the provided [`MessageOrigin`].
fn prioritize_topics(origin: MessageOrigin, config: &SynchronizerConfig) -> (String, Vec<String>) {
    let controls_topic = config.controls_topic.clone();
    let phoebus_topic = config.phoebus_topics[0].clone();
    let phoebus_command_topic = get_command_topic(&phoebus_topic);

    let send_topic = match origin {
        MessageOrigin::Controls => controls_topic.clone(),
        MessageOrigin::Phoebus => phoebus_topic.clone(),
        MessageOrigin::PhoebusCommand => phoebus_command_topic.clone(),
    };
    (
        send_topic,
        vec![controls_topic, phoebus_topic, phoebus_command_topic],
    )
}

/// Produces the specified [`Message`] on the [`Harness`]'s host and topic.
async fn send_test_message(
    harness: &Harness,
    message: Message,
    send_topic: String,
) -> Result<(), Box<dyn Error>> {
    let sender = KafkaPublisher::new(harness.host(), send_topic.clone());
    sender.publish(message).await?;
    info!("message sent to {}", send_topic);
    Ok(())
}

/// Helper method that sends the message to induce the behavior being tested and checks to see
///  whether the desired outcome was observed.
async fn do_test(
    harness: Harness,
    message: Message,
    send_topic: String,
    condition: impl AsyncFnMut() -> bool,
) -> Result<(), Box<dyn Error>> {
    send_test_message(&harness, message, send_topic).await?;
    wait_for_condition(condition).await
}

/// Loops indefinitely while checking to see if the provided [`condition`](AsyncFnMut) has been met.
/// Rechecks the condition every 100ms.
async fn try_condition(mut condition: impl AsyncFnMut() -> bool) {
    loop {
        if condition().await {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Wraps [`try_condition`] in a [`timeout`] to fail the test if the [`condition`](AsyncFnMut) is
/// not met within 10 seconds.
async fn wait_for_condition(condition: impl AsyncFnMut() -> bool) -> Result<(), Box<dyn Error>> {
    Ok(timeout(Duration::from_secs(10), try_condition(condition)).await?)
}

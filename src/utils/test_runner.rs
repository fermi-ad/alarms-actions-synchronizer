//! Test Runner Module
//!
//! Contains functions and constants centered around the [`TestRunner`] struct.
//! [`TestRunner`] descibes the common flow of a test case: some data appears on a given
//! Kafka topic and the [`Synchronizer`] under test is expected to respond in a certain way.

use crate::models::{Synchronizer, SynchronizerConfig, metadata::MetadataScope};
use rust_pubsub_lib::{
    KafkaPublisher, KafkaSnapshot, KafkaSubscriber, KafkaTestHarness, Message, Publisher,
    StringMessage,
};
use std::error::Error;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

/// The topic used in the tests for messages from Controls.
pub const CONTROLS_TOPIC: &str = "controls";

/// A sentinel device name used by test-only runtime readiness probes.
const READINESS_DEVICE: &str = "__test_runner_readiness_device__";

/// The topic used in the tests for messages to/from Phoebus.
pub const PHOEBUS_TOPIC: &str = "phoebus";

/// Describes the origin of the [`Message`] that initiates the behavior being tested.
///
/// Used by [`TestRunner`] to choose which topic from the [`SynchronizerConfig`] to send the message on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageOrigin {
    Controls,
    Phoebus,
}

/// Generates a [`SynchronizerConfig`] instance where the Kafka topics are the default testing
/// [`CONTROLS_TOPIC`] and [`PHOEBUS_TOPIC`].
fn get_mock_sync_config() -> SynchronizerConfig {
    SynchronizerConfig::new(
        CancellationToken::new(),
        String::new(),
        String::from(CONTROLS_TOPIC),
        String::new(),
        String::new(),
        vec![String::from(PHOEBUS_TOPIC)],
    )
}

/// A helper tool that simulates a [`Synchronizer`] receiving the provided [`Message`] and checks that the expected behavior ensues.
pub struct TestRunner<M, N, T>
where
    M: Message<N>,
    T: Synchronizer<KafkaPublisher, KafkaSubscriber> + Send + Sync + 'static,
{
    cancel_token: CancellationToken,
    pub test_config: SynchronizerConfig,
    /// The [`KafkaTestHarness`] containing the location of the test Kafka Cluster via its [`host()`](KafkaTestHarness::host) method.
    pub harness: KafkaTestHarness,
    message: Option<M>,
    _message_type: PhantomData<N>,
    send_topic: String,
    /// The [`Synchronizer`] instance being tested.
    pub sync: T,
}
impl<M: Message<N>, N, T: Synchronizer<KafkaPublisher, KafkaSubscriber> + Send + Sync + 'static>
    TestRunner<M, N, T>
{
    /// Generates a [`TestInstance`] for a [`Synchronizer`] that listens to messages from [`MessageOrigin`].
    ///
    /// A [`Message`] is simulated to be sent on the appropriate topic by calling [`has`](Self::has).
    ///
    /// The results of the test are then determined by calling one of
    /// - [`results_in`](Self::results_in)
    /// - [`after_init_results_in`](Self::after_init_results_in)
    pub async fn check_when(origin: MessageOrigin) -> Self {
        let mut config = get_mock_sync_config();

        let cancel_token = config.cancel_token.clone();

        let harness = KafkaTestHarness::with_topics(Vec::new()).await;
        rewrite_config_topics_with_harness_topics(&harness, &mut config).await;
        let send_topic = prioritized_send_topic(origin, &config);

        config.controls_host = harness.host().await;
        config.phoebus_host = config.controls_host.clone();
        let sync = T::new(config.clone());

        TestRunner {
            cancel_token,
            test_config: config,
            harness,
            message: None,
            _message_type: PhantomData,
            send_topic,
            sync,
        }
    }

    /// Supplies the [`Message`] to send that kicks off the test case.
    pub fn has(mut self, message: M) -> Self {
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
        condition: impl AsyncFnMut() -> bool,
    ) -> Result<(), Box<dyn Error>> {
        let metadata_scope = self.test_config.metadata_scope.clone();

        // Asynchronously kick off the synchronizer in a separate task
        tokio::spawn(self.sync.synchronize::<KafkaSnapshot>());

        await_phoebus_runtime_ready(&self.harness, &self.test_config, &metadata_scope).await;

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
}

/// Rewrites the synchronizer config so each test uses fresh Kafka topics allocated by the shared harness.
///
/// [`KafkaTestHarness`](rust-pubsub-lib) uses a global mock cluster, so fixed topic names are visible across test
/// cases. Harness-generated unique topics keep startup hydration and runtime monitoring isolated to messages from the
/// current test.
async fn rewrite_config_topics_with_harness_topics(
    harness: &KafkaTestHarness,
    config: &mut SynchronizerConfig,
) {
    config.controls_topic = KafkaTestHarness::new_topic(CONTROLS_TOPIC).await;

    let mut phoebus_topics = Vec::with_capacity(config.phoebus_topics.len());
    for topic in &config.phoebus_topics {
        phoebus_topics.push(KafkaTestHarness::new_topic(topic).await);
    }
    config.phoebus_topics = phoebus_topics;

    // Ensure all allocated topics exist on the same shared mock cluster host referenced by this harness handle.
    let _ = harness.host().await;
}

/// Determines which topic the injected test message should be sent on for the provided [`MessageOrigin`].
fn prioritized_send_topic(origin: MessageOrigin, config: &SynchronizerConfig) -> String {
    let phoebus_topic = config.phoebus_topics[0].clone();
    match origin {
        MessageOrigin::Controls => config.controls_topic.clone(),
        MessageOrigin::Phoebus => phoebus_topic,
    }
}

/// Produces the specified [`Message`] on the [`Harness`]'s host and topic.
pub async fn send_test_message<N, M: Message<N>>(
    harness: &KafkaTestHarness,
    message: M,
    send_topic: String,
) -> Result<(), Box<dyn Error>> {
    let sender = KafkaPublisher::new(harness.host().await, send_topic);
    sender.publish(message).await?;
    Ok(())
}

/// Helper method that sends the message to induce the behavior being tested and checks to see
///  whether the desired outcome was observed.
async fn do_test<N, M: Message<N>>(
    harness: KafkaTestHarness,
    message: M,
    send_topic: String,
    condition: impl AsyncFnMut() -> bool,
) -> Result<(), Box<dyn Error>> {
    send_test_message(&harness, message, send_topic.clone()).await?;
    wait_for_condition(condition).await
}

/// Sends a benign Phoebus config message that causes the runtime monitor to create a predictable local cache entry.
pub async fn await_phoebus_runtime_ready(
    harness: &KafkaTestHarness,
    test_config: &SynchronizerConfig,
    metadata_scope: &MetadataScope,
) {
    let readiness_message = StringMessage::new(
        Some(format!("config:runtime/readiness/{READINESS_DEVICE}")),
        serde_json::to_string(&crate::models::phoebus::Config::default())
            .expect("Failed to serialize Phoebus runtime readiness config."),
    );

    send_test_message(
        harness,
        readiness_message,
        test_config.phoebus_topics[0].clone(),
    )
    .await
    .expect("Failed to publish Phoebus runtime readiness probe message.");

    wait_for_condition(async || {
        metadata_scope
            .lookup_metadata_by_device(READINESS_DEVICE)
            .await
            .is_some()
    })
    .await
    .expect("Phoebus runtime readiness probe did not populate test metadata cache.");
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

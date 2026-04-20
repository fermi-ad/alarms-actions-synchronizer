//! Phoebus Monitor Module
//!
//! Contains the construct that watches a given Phoebus topic and reports relevant messages to Controls.

use crate::{
    models::{
        ACK_COMMAND, AlarmStateCache, CachedState, PvCache, SynchronizerConfig,
        alarm::status::State,
        phoebus::{Command, Config, Key, Operation, PvMetadata},
    },
    phoebus::sync::ControlsClient,
};
use rust_pubsub_lib::{Message, PubSubError, Subscriber};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct Monitor {
    /// The atomic cache of alarm state data.
    alarm_states: AlarmStateCache,

    /// A token to watch in case the parent process is cancelled.
    cancel_token: CancellationToken,

    /// The client for passing alarms info to the Controls alarms service.
    controls_client: ControlsClient,

    /// The location of the Phoebus alarms topics. Used during inital startup of the sync operation.
    phoebus_host: String,

    /// The atomic cache of PV metadata.
    pv_metadata: PvCache,

    /// The topic to monitor in the Phoebus Kafka.
    topic: String,
}
impl Monitor {
    /// Generates a [`Monitor`] for the provided topic.
    pub fn new(
        topic: String,
        config: &SynchronizerConfig,
        controls_client: ControlsClient,
    ) -> Self {
        Monitor {
            alarm_states: Arc::clone(&config.alarm_states),
            cancel_token: config.cancel_token.clone(),
            controls_client,
            phoebus_host: config.phoebus_host.clone(),
            pv_metadata: Arc::clone(&config.pv_metadata),
            topic,
        }
    }

    /// Kicks off the asynchronous monitoring of the topic and handling of messages that appear there.
    pub async fn start<S: Subscriber>(self) {
        info!("Starting monitor for Phoebus topic: {}", self.topic);
        loop {
            let mut sub = S::new(self.phoebus_host.clone(), self.topic.clone());
            match sub.get_stream().as_mut() {
                Ok(phoebus_stream) => self.watch_stream(phoebus_stream).await,
                Err(e) => error!("{e}"),
            }

            if self.cancel_token.is_cancelled() {
                return;
            }

            warn!(
                "Stream for Phoebus topic {} was dropped. Attempting to reconnect.",
                self.topic
            );
            sleep(Duration::from_secs(1)).await;
        }
    }

    /// Looks up the corresponding [`PvMetadata`] for a given [`Key`], or creates one if none exists.
    async fn get_pv_metadata(&self, key: &Key) -> PvMetadata {
        let metadata_opt = self.pv_metadata.read().await.get(&key.device).cloned();

        metadata_opt.unwrap_or_else(|| PvMetadata {
            config: Config::default(),
            display_path: key.display_path.clone(),
            phoebus_topic: self
                .topic
                .strip_suffix("Command")
                .unwrap_or(&self.topic)
                .to_owned(),
        })
    }

    /// Handles when a config came in from Phoebus to activate a bypassed alarm.
    async fn handle_active_alarm(&self, device: &str, updated_state: CachedState) {
        let cached_state = self.alarm_states.read().await.get(device).cloned();
        if let Some(state) = cached_state
            && state.state != State::Bypassed
        {
            handle_already_active(device);
        } else {
            info!(
                "TODO: Update the alarms serivce with an Ok condition for device '{}'",
                device
            );
            self.alarm_states
                .write()
                .await
                .insert(device.to_owned(), updated_state);
        }
    }

    /// Handles when a config came in from Phoebus to bypass an active alarm.
    async fn handle_bypassed_alarm(&self, device: &str, updated_state: CachedState, user: &str) {
        let cached_state = self.alarm_states.read().await.get(device).cloned();
        if let Some(state) = cached_state
            && state == updated_state
        {
            info!(
                "Received configuration update from Phoebus to bypass alarm for device '{}', but it is already bypassed. Updating cached PV config only.",
                device
            );
            return;
        }
        match updated_state.wake {
            Some(time) => self.controls_client.snooze_alarm(device, user, time).await,
            None => self.controls_client.bypass_alarm(device, user).await,
        }
        self.alarm_states
            .write()
            .await
            .insert(device.to_owned(), updated_state);
    }

    /// Handles a Command message coming in from Phoebus.
    async fn process_command(&self, key: Key, msg_text: String) {
        let command_msg = match serde_json::from_str::<Command>(&msg_text) {
            Ok(inner) => inner,
            Err(e) => {
                error!(
                    "Failed to deserialize Phoebus command: {e}\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
                );
                return;
            }
        };
        if command_msg.command != ACK_COMMAND {
            debug!(
                "Received Phoebus command that does not need to be processed. Doing nothing.\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
            );
            return;
        }
        if let Some(cur_state) = self.alarm_states.read().await.get(&key.device)
            && cur_state.state == State::Acknowledged
        {
            info!(
                "Received acknowledgement command from Phoebus for device '{}', but it is already acknowledged. Doing nothing.",
                key.device
            );
            return;
        }
        self.controls_client
            .acknowledge_alarm(&key.device, &command_msg.user)
            .await;
        self.alarm_states.write().await.insert(
            key.device,
            CachedState {
                state: State::Acknowledged,
                wake: None,
            },
        );
    }

    /// Handles a message from Phoebus that updates the configuration of a PV.
    async fn process_config(&self, key: Key, msg_text: String) {
        let config_msg = match serde_json::from_str::<Config>(&msg_text) {
            Ok(inner) => inner,
            Err(e) => {
                error!(
                    "Failed to deserialize Phoebus config: {e}\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
                );
                return;
            }
        };
        let cached_metadata = self.get_pv_metadata(&key).await;
        if config_msg == cached_metadata.config {
            info!(
                "Received config from Phoebus for device '{}' that matches the cached config. Doing nothing.",
                key.device
            );
            return;
        }
        if config_msg.enabled != cached_metadata.config.enabled {
            let updated_state = config_msg.as_cached_state();
            if updated_state.state == State::Bypassed {
                self.handle_bypassed_alarm(&key.device, updated_state, &config_msg.user)
                    .await;
            } else if updated_state.state == State::Ok {
                self.handle_active_alarm(&key.device, updated_state).await;
            } else {
                error!("Could not determine state of new config message: {config_msg:?}");
            }
        }
        self.pv_metadata.write().await.insert(
            key.device.clone(),
            PvMetadata {
                config: config_msg,
                ..cached_metadata
            },
        );
    }

    /// The primary logic for handling a new message from Phoebus.
    /// Disambiguates the type of message and hands it off to the appropriate helper method.
    async fn process_message(&self, msg: Message) {
        if msg.key.is_none() {
            error!(
                "Got message with no key. There is a problem with the pub-sub crate or with the messages in the Phoebus Kafka.\n Message: {msg:?}"
            );
            return;
        }
        let key = Key::from(msg.key.unwrap());
        match key.operation {
            Operation::Command => self.process_command(key, msg.value).await,
            Operation::Config => self.process_config(key, msg.value).await,
            Operation::Other => process_other(key, msg.value),
        }
    }

    /// Monitors the provided [`Stream`] and processes messages that appear there. Terminates when the stream ends or a cancel is requested.
    async fn watch_stream(
        &self,
        phoebus_stream: &mut (impl Stream<Item = Result<Message, PubSubError>> + Unpin + Send),
    ) {
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                stream_item = phoebus_stream.next() => {
                    match stream_item {
                        Some(stream_result) => {
                            match stream_result {
                                Ok(message) => self.process_message(message).await,
                                Err(e) => warn!("Error from within data stream: {e}"),
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    }
}

/// Handles when a message comes in to activate a device that was already thought to be active.
fn handle_already_active(device: &str) {
    info!(
        "Received configuration update from Phoebus to activate alarm for device '{device}', but it is already active. Updating cached config only."
    );
}

/// Handles when a message is not a command or a config.
fn process_other(key: Key, msg_text: String) {
    debug!(
        "Received Phoebus message that is not a config or a command. Doing nothing.\n Original message from Phoebus: {{ key: {key:?}, text: {msg_text} }}"
    )
}

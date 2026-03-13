//! Phoebus Module Tests

use super::*;
use crate::{
    models::{
        ACK_COMMAND, CachedState,
        alarm::status::State,
        phoebus::{Command, Config, PvMetadata},
    },
    utils::test_runner::{
        MessageOrigin, TestRunner, get_mock_sync_config, get_mock_sync_config_salted,
    },
};
use chrono::Utc;
use rust_pubsub_lib::{
    Message,
    kafka_impl::{KafkaPublisher, KafkaSnapshot, KafkaSubscriber},
};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;

#[tokio::test]
#[tracing_test::traced_test]
async fn should_create_new_synchronizer() {
    let config = get_mock_sync_config();
    let cancel_token = config.cancel_token.clone();
    tokio::spawn(async move {
        let sync: SyncImpl = Synchronizer::<KafkaPublisher, KafkaSubscriber>::new(config);
        Synchronizer::<KafkaPublisher, KafkaSubscriber>::synchronize::<KafkaSnapshot>(sync).await
    });

    // Give things a beat to start.
    sleep(Duration::from_millis(250)).await;
    cancel_token.cancel();

    assert!(logs_contain("Starting Phoebus-to-Controls Synchronizer"));
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_messages_without_keys_on_init() {
    let message = Message {
        key: None,
        value: String::new(),
    };

    TestRunner::<SyncImpl>::check_when(MessageOrigin::Phoebus, None)
        .await
        .has(message)
        .on_init_results_in(async || logs_contain("No key provided on config/state message:"))
        .await
        .expect("The expected log message was never recorded");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_messages_without_keys_after_init() {
    let message = Message {
        key: None,
        value: String::new(),
    };
    let sync_config = get_mock_sync_config_salted();
    let phoebus_topic = sync_config.phoebus_topics[0].clone();
    TestRunner::<SyncImpl>::check_when(MessageOrigin::Phoebus, Some(sync_config))
        .await
        .has(message)
        .after_init_results_in(
            async || logs_contain(&format!("Topic {phoebus_topic} has no messages")),
            async || logs_contain("Got message with no key."),
        )
        .await
        .expect("The expected log message was never recorded");
}

#[tokio::test]
async fn should_sync_valid_acknowledge_commands() {
    let test_instance =
        TestRunner::<SyncImpl>::check_when(MessageOrigin::PhoebusCommand, None).await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let command = Command {
        user: String::from("my user"),
        host: String::new(),
        command: ACK_COMMAND.to_string(),
    };
    let message = Message {
        key: Some(String::from("command:my/path/to/MyDevice")),
        value: serde_json::to_string(&command).unwrap(),
    };

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Acknowledged)
        })
        .await
        .expect("The alarm state was not set to 'Acknowledged'")
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_unparseable_command_messages() {
    let message = Message {
        key: Some(String::from("command:not/a/real/Device")),
        value: String::from("{ \"fakeKey\": \"Can't be parsed to a Command object\" }"),
    };

    TestRunner::<SyncImpl>::check_when(MessageOrigin::PhoebusCommand, None)
        .await
        .has(message)
        .results_in(async || logs_contain("Failed to deserialize Phoebus command"))
        .await
        .expect("The expected log message was never recorded");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_invalid_commands() {
    let command = Command {
        user: String::from("my user"),
        host: String::new(),
        command: String::from("some other command"),
    };
    let message = Message {
        key: Some(String::from("command:my/path/to/MyDevice")),
        value: serde_json::to_string(&command).unwrap(),
    };

    TestRunner::<SyncImpl>::check_when(MessageOrigin::PhoebusCommand, None)
        .await
        .has(message)
        .results_in(async || {
            logs_contain(
                "Received Phoebus command that does not need to be processed. Doing nothing.",
            )
        })
        .await
        .expect("The expected log message was never recorded");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_already_synced_commands() {
    let test_instance =
        TestRunner::<SyncImpl>::check_when(MessageOrigin::PhoebusCommand, None).await;
    let sync = &test_instance.sync;
    sync.config.alarm_states.write().await.insert(
        String::from("MyDevice"),
        CachedState {
            state: State::Acknowledged,
            wake: None,
        },
    );

    let command = Command {
        user: String::from("my user"),
        host: String::new(),
        command: ACK_COMMAND.to_string(),
    };
    let message = Message {
        key: Some(String::from("command:my/path/to/MyDevice")),
        value: serde_json::to_string(&command).unwrap(),
    };

    test_instance.has(message)
            .results_in(async || {
                logs_contain(
                    "Received acknowledgement command from Phoebus for device 'MyDevice', but it is already acknowledged. Doing nothing.",
                )
            })
            .await
            .expect("The expected log message was never recorded");
}

#[tokio::test]
async fn should_sync_valid_bypass_config_with_false() {
    let test_instance = TestRunner::<SyncImpl>::check_when(
        MessageOrigin::Phoebus,
        Some(get_mock_sync_config_salted()),
    )
    .await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let mut config = Config::default();
    config.enabled = Some(true.to_string());

    sync.config.pv_metadata.write().await.insert(
        String::from("MyDevice"),
        PvMetadata {
            config: config.clone(),
            display_path: String::from("my/path/to"),
            phoebus_topic: sync.config.phoebus_topics[0].clone(),
        },
    );

    config.enabled = Some(false.to_string());

    let message = Message {
        key: Some(String::from("config:my/path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Bypassed)
        })
        .await
        .expect("The alarm state was not set to 'Bypassed'");
}

#[tokio::test]
async fn should_sync_valid_bypass_config_with_none() {
    let test_instance = TestRunner::<SyncImpl>::check_when(
        MessageOrigin::Phoebus,
        Some(get_mock_sync_config_salted()),
    )
    .await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let mut config = Config::default();
    config.enabled = Some(true.to_string());

    sync.config.pv_metadata.write().await.insert(
        String::from("MyDevice"),
        PvMetadata {
            config: config.clone(),
            display_path: String::from("my/path/to"),
            phoebus_topic: sync.config.phoebus_topics[0].clone(),
        },
    );

    config.enabled = None;

    let message = Message {
        key: Some(String::from("config:my/path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Bypassed)
        })
        .await
        .expect("The alarm state was not set to 'Bypassed'");
}

#[tokio::test]
async fn should_sync_valid_snooze_config() {
    let test_instance = TestRunner::<SyncImpl>::check_when(
        MessageOrigin::Phoebus,
        Some(get_mock_sync_config_salted()),
    )
    .await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let mut config = Config::default();
    config.enabled = Some(true.to_string());

    sync.config.pv_metadata.write().await.insert(
        String::from("MyDevice"),
        PvMetadata {
            config: config.clone(),
            display_path: String::from("my/path/to"),
            phoebus_topic: sync.config.phoebus_topics[0].clone(),
        },
    );

    config.enabled = Some((Utc::now() + Duration::from_hours(1)).to_rfc3339());

    let message = Message {
        key: Some(String::from("config:my/path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Bypassed && state.wake.is_some())
        })
        .await
        .expect("The alarm state was not set to 'Bypassed' or the state's wake value was not set");
}
#[tokio::test]
async fn should_sync_valid_active_config_with_time() {
    let test_instance = TestRunner::<SyncImpl>::check_when(
        MessageOrigin::Phoebus,
        Some(get_mock_sync_config_salted()),
    )
    .await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    sync.config.pv_metadata.write().await.insert(
        String::from("MyDevice"),
        PvMetadata {
            config: Config::default(),
            display_path: String::from("my/path/to"),
            phoebus_topic: sync.config.phoebus_topics[0].clone(),
        },
    );

    let mut config = Config::default();
    config.enabled = Some((Utc::now() - Duration::from_hours(1)).to_rfc3339());

    let message = Message {
        key: Some(String::from("config:my/path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Ok)
        })
        .await
        .expect("The alarm state was not set to 'Ok'");
}

#[tokio::test]
async fn should_sync_valid_active_config_with_true() {
    let test_instance = TestRunner::<SyncImpl>::check_when(
        MessageOrigin::Phoebus,
        Some(get_mock_sync_config_salted()),
    )
    .await;
    let sync = &test_instance.sync;

    let alarm_states = Arc::clone(&sync.config.alarm_states);

    let mut config = Config::default();
    config.enabled = Some(true.to_string());

    let message = Message {
        key: Some(String::from("config:my/path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };

    test_instance
        .has(message)
        .results_in(async || {
            alarm_states
                .read()
                .await
                .get("MyDevice")
                .is_some_and(|state| state.state == State::Ok)
        })
        .await
        .expect("The alarm state was not set to 'Ok'");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_corrupted_config_on_init() {
    let message = Message {
        key: Some(String::from("config:path/to/MyDevice")),
        value: String::from("{ \"notRealConfigMessage\": \"Should not parse\" }"),
    };
    TestRunner::<SyncImpl>::check_when(MessageOrigin::Phoebus, None)
        .await
        .has(message)
        .on_init_results_in(async || logs_contain("Failed deserializing config message"))
        .await
        .expect("The expected log message was not detected.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_corrupted_config_after_init() {
    let message = Message {
        key: Some(String::from("config:path/to/MyDevice")),
        value: String::from("{ \"notRealConfigMessage\": \"Should not parse\" }"),
    };
    let sync_config = get_mock_sync_config_salted();
    let phoebus_topic = sync_config.phoebus_topics[0].clone();
    TestRunner::<SyncImpl>::check_when(MessageOrigin::Phoebus, Some(sync_config))
        .await
        .has(message)
        .after_init_results_in(
            async || logs_contain(&format!("Topic {phoebus_topic} has no messages")),
            async || logs_contain("Failed to deserialize Phoebus config"),
        )
        .await
        .expect("The expected log message was not detected.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_duplicated_config() {
    let sync_config = get_mock_sync_config_salted();
    let phoebus_topic = sync_config.phoebus_topics[0].clone();
    let test_instance =
        TestRunner::<SyncImpl>::check_when(MessageOrigin::Phoebus, Some(sync_config)).await;
    let sync = &test_instance.sync;

    let config = Config::default();

    sync.config.pv_metadata.write().await.insert(
        String::from("MyDevice"),
        PvMetadata {
            config: config.clone(),
            display_path: String::new(),
            phoebus_topic: phoebus_topic.clone(),
        },
    );

    let message = Message {
        key: Some(String::from("config:path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };
    test_instance
            .has(message)
            .after_init_results_in(
                async || logs_contain(&format!("Topic {phoebus_topic} has no messages")),
                async || logs_contain("Received config from Phoebus for device 'MyDevice' that matches the cached config. Doing nothing."),
            )
            .await
            .expect("The expected log message was not detected.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_unexpected_enabled_states() {
    let test_instance = TestRunner::<SyncImpl>::check_when(MessageOrigin::Phoebus, None).await;
    let sync = &test_instance.sync;

    let mut config = Config::default();

    sync.config.pv_metadata.write().await.insert(
        String::from("MyDevice"),
        PvMetadata {
            config: config.clone(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        },
    );

    config.enabled = Some(String::from("invalid value"));

    let message = Message {
        key: Some(String::from("config:path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };
    test_instance
            .has(message)
            .results_in(async || logs_contain("Could not parse the enabled state of a Phoebus config message to either a date or a bool."))
            .await
            .expect("The expected log message was not detected.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_already_active_config() {
    let test_instance = TestRunner::<SyncImpl>::check_when(
        MessageOrigin::Phoebus,
        Some(get_mock_sync_config_salted()),
    )
    .await;
    let sync = &test_instance.sync;
    let phoebus_topic = sync.config.phoebus_topics[0].clone();

    let mut config = Config::default();

    sync.config.pv_metadata.write().await.insert(
        String::from("MyDevice"),
        PvMetadata {
            config: config.clone(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        },
    );
    sync.config.alarm_states.write().await.insert(
        String::from("MyDevice"),
        CachedState {
            state: State::Ok,
            wake: None,
        },
    );

    config.enabled = Some(true.to_string());

    let message = Message {
        key: Some(String::from("config:path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };
    test_instance.has(message)
            .after_init_results_in(
                async || logs_contain(&format!("Topic {phoebus_topic} has no messages")),
                async || logs_contain("Received configuration update from Phoebus to activate alarm for device 'MyDevice', but it is already active. Updating cached config only.")
            )
            .await
            .expect("The expected log message was not detected.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_already_bypassed_config() {
    let test_instance = TestRunner::<SyncImpl>::check_when(
        MessageOrigin::Phoebus,
        Some(get_mock_sync_config_salted()),
    )
    .await;
    let sync = &test_instance.sync;
    let phoebus_topic = sync.config.phoebus_topics[0].clone();

    let mut config = Config::default();
    config.enabled = Some(true.to_string());

    sync.config.pv_metadata.write().await.insert(
        String::from("MyDevice"),
        PvMetadata {
            config: config.clone(),
            display_path: String::new(),
            phoebus_topic: String::new(),
        },
    );
    sync.config.alarm_states.write().await.insert(
        String::from("MyDevice"),
        CachedState {
            state: State::Bypassed,
            wake: None,
        },
    );

    config.enabled = Some(false.to_string());

    let message = Message {
        key: Some(String::from("config:path/to/MyDevice")),
        value: serde_json::to_string(&config).unwrap(),
    };
    test_instance.has(message)
            .after_init_results_in(
                async || logs_contain(&format!("Topic {phoebus_topic} has no messages")),
                async || logs_contain("Received configuration update from Phoebus to bypass alarm for device 'MyDevice', but it is already bypassed. Updating cached PV config only.")
            )
            .await
            .expect("The expected log message was not detected.");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn should_not_sync_unknown_operations() {
    let message = Message {
        key: Some(String::from("some-other-command:path/to/MyDevice")),
        value: String::new(),
    };
    let sync_config = get_mock_sync_config_salted();
    let phoebus_topic = sync_config.phoebus_topics[0].clone();
    TestRunner::<SyncImpl>::check_when(MessageOrigin::Phoebus, Some(sync_config))
        .await
        .has(message)
        .after_init_results_in(
            async || logs_contain(&format!("Topic {phoebus_topic} has no messages")),
            async || {
                logs_contain(
                    "Received Phoebus message that is not a config or a command. Doing nothing.",
                )
            },
        )
        .await
        .expect("The expected log message was not detected.");
}

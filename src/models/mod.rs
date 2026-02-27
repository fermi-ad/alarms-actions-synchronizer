//! Models Module
//!
//! Provides the common data structures used throughout the application.

pub use common::alarm;
pub use google::protobuf as generated;
use rust_pubsub_lib::{Publisher, Subscriber};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

/// The command that will come in from/should be sent to Phoebus during an acknowledgement.
pub const ACK_COMMAND: &str = "acknowledge";

/// Alias for the atomic cache of alarm state data, shared between the two synchronizing processes.
pub type AlarmStateCache = Arc<RwLock<HashMap<String, CachedState>>>;

/// Alias for the atomic cache of PV metadata shared between the two synchronizing processes.
pub type PvCache = Arc<RwLock<HashMap<String, phoebus::PvMetadata>>>;

pub mod phoebus {
    //! Phoebus Module
    //!
    //! Contains data structures that are germane to the Phoebus environment.

    /// A struct representing a message from the Command topic.
    ///
    /// Used in the Phoebus context to acknowledge alarms.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Command {
        /// The user issuing the command.
        pub user: String,

        /// The host where the command originated.
        pub host: String,

        /// The command itself.
        pub command: String,
    }

    /// A struct representing a configuration message on the main Phoebus topic.
    ///
    /// Used in the Phoebus context to enable, bypass, and snooze alarms.
    ///
    /// A field set to [`None`] indicates `false`, or that the field should be ignored.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Config {
        /// The user setting the new configuration.
        pub user: String,

        /// The host the user is making the change from.
        pub host: String,

        /// The enabled state of the alarm.
        ///
        /// This is either a time or a boolean - represented as a string to handle the ambiguity. Thanks EPICS.
        pub enabled: Option<String>,

        // The remaining values are all relevant to the Phoebus environment, but will have no bearing on the operation of this application.
        // They are modeled here so that updates to the `enabled` field do not erase other configuration settings.
        pub latching: Option<bool>,
        pub annunciating: Option<bool>,
        pub delay: Option<i64>,
        pub count: Option<i64>,
        pub filter: Option<String>,
        pub guidance: Option<Vec<TitleDetails>>,
        pub displays: Option<Vec<TitleDetails>>,
        pub commands: Option<Vec<TitleDetails>>,
        pub actions: Option<Vec<TitleDetails>>,
    }

    /// This struct is a convenience for parsing the key of a Phoebus Kafka message.
    #[derive(Debug)]
    pub struct Key {
        /// An [`Operation`] representing the first characters of the key string,
        /// everything before the first `:` character.
        pub operation: Operation,

        /// The middle part of the key string, describing the path to the alarm in the Phoebus display.
        pub display_path: String,

        /// The name of the PV (or 'device'); the last part of the key string. Everything after the final '/' character.
        pub device: String,
    }
    impl From<String> for Key {
        fn from(value: String) -> Self {
            // The device name will be everything after the final `/` character. Use reverse split to extract it more easily.
            let (prefix, device) = value.rsplit_once("/").unwrap();
            // The operation (config, command, etc.) is encoded as all the text before the first `:` character.
            let (operation_str, display_path) = prefix.split_once(":").unwrap();
            Key {
                operation: Operation::from(operation_str),
                display_path: display_path.to_owned(),
                device: device.to_owned(),
            }
        }
    }

    /// Encapsulates the various operations from Phoebus that this sync service will handle.
    #[derive(Debug, Eq, PartialEq)]
    pub enum Operation {
        Command,
        Config,
        Other,
    }
    impl Operation {
        /// Generates the prefix for the Kafka message key that is relevant to the current operation type.
        pub fn get_key_prefix(&self) -> &'static str {
            match self {
                Operation::Command => "command",
                Operation::Config => "config",
                Operation::Other => "",
            }
        }

        /// Provides a [`String`] to use when an attempt is made to operate on an [`Other`](Self::Other) operation.
        pub fn get_err_string_for_other() -> String {
            "Cannot operate on type 'Other'".to_string()
        }
    }
    impl From<&str> for Operation {
        fn from(value: &str) -> Self {
            match value {
                "command" => Operation::Command,
                "config" => Operation::Config,
                _ => Operation::Other,
            }
        }
    }

    /// Metadata to track about individual PV alarms.
    /// Allows the sync service to push updates to Phoebus without damaging other parts of the alarm configuration.
    #[derive(Clone, Debug)]
    pub struct PvMetadata {
        /// The last configuration record received for this PV. Preserved so future updates to the enabled state of the alarm
        /// do not erase other config data.
        pub config: Config,

        /// The path to the PV in the Phoebus display. Extracted from the config message key.
        pub display_path: String,

        /// The topic that this PV's alarms appear in.
        pub phoebus_topic: String,
    }

    /// A sub-element of a Phoebus configuration record. Not relevant to this application,
    /// but modeled so it is preserved when this service pushes updates to Phoebus.
    #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct TitleDetails {
        pub title: String,
        pub details: String,
        pub delay: Option<String>,
    }
}

/// Encapsulates the latest state information about an alarm.
#[derive(Clone, Debug)]
pub struct CachedState {
    /// The latest [`State`](alarm::status::State) the sync service has recorded for the alarm.
    pub state: alarm::status::State,
    /// If the alarm is snoozed, this field will be set to [`Some`] with the reenablement time, and
    /// the [`state`](Self::state) field will be set to [`Bypassed`](alarm::status::State::Bypassed).
    /// Otherwise, this field will be [`None`].
    pub wake: Option<generated::Timestamp>,
}
impl From<alarm::Status> for CachedState {
    fn from(value: alarm::Status) -> Self {
        CachedState {
            state: value.state(),
            wake: value.wake,
        }
    }
}

/// Configuration data to initialize the synchronizer processes.
#[derive(Debug)]
pub struct SynchronizerConfig {
    /// A reference to the shared cache of alarm state data.
    pub alarm_states: AlarmStateCache,

    /// The location of the Controls Kafka instance.
    pub controls_host: String,

    /// The topic to read/write Controls messages from/to.
    pub controls_topic: String,

    /// The location of the Phoebus Kafka instance.
    pub phoebus_host: String,

    /// The [`Vec`] of topics to read/write from/to for Phoebus messages.
    ///
    /// This will just contain the base names of the topics. That is, Phoebus requires
    /// each "topic" be split into 3 parts: a vanilla topic to hold the state and config records,
    /// a "Command" topic for clients to send commands to the service, and a "Talk" topic for the service
    /// to send messages for annunciation.
    ///
    /// Each instance of [`Synchronizer`] will determine which, if any, of the auxiliary topics it will interact with.
    pub phoebus_topics: Vec<String>,

    /// A reference to the shared cache of PV metadata.
    pub pv_metadata: PvCache,
}
impl SynchronizerConfig {
    /// Creates a new instance of [`SynchronizerConfig`] from the provided hosts and topics.
    ///
    /// As part of the initialization, this constructor will generate the shared atomic caches
    /// on the heap.
    pub fn new(
        controls_host: String,
        controls_topic: String,
        phoebus_host: String,
        phoebus_topics: Vec<String>,
    ) -> Self {
        SynchronizerConfig {
            alarm_states: Arc::new(RwLock::new(HashMap::<String, CachedState>::new())),
            controls_host,
            controls_topic,
            phoebus_host,
            phoebus_topics,
            pv_metadata: Arc::new(RwLock::new(HashMap::<String, phoebus::PvMetadata>::new())),
        }
    }
}
impl Clone for SynchronizerConfig {
    /// Generates a new instance with copies of the references to the shared caches.
    /// That is, this will NOT create new caches on the heap, but references the same
    /// instances as the [`SynchronizerConfig`] instance being cloned.
    fn clone(&self) -> Self {
        SynchronizerConfig {
            alarm_states: Arc::clone(&self.alarm_states),
            controls_host: self.controls_host.clone(),
            controls_topic: self.controls_topic.clone(),
            phoebus_host: self.phoebus_host.clone(),
            phoebus_topics: self.phoebus_topics.clone(),
            pv_metadata: Arc::clone(&self.pv_metadata),
        }
    }
}
impl PartialEq for SynchronizerConfig {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.alarm_states, &other.alarm_states)
            && self.controls_host == other.controls_host
            && self.controls_topic == other.controls_topic
            && self.phoebus_host == other.phoebus_host
            && self.phoebus_topics == other.phoebus_topics
            && Arc::ptr_eq(&self.pv_metadata, &other.pv_metadata)
    }
}

/// A trait to describe the basic functions of a synchronization process.
#[async_trait::async_trait]
pub trait Synchronizer<P: Publisher, S: Subscriber> {
    /// Constructs a [`Synchronizer`] instance from the provided [`SynchronizerConfig`] instance.
    fn new(config: SynchronizerConfig) -> Self;

    /// Kicks off the async process to monitor for alarm updates that need synchronization.
    async fn synchronize(&mut self);
}

mod common {
    pub mod alarm {
        //! Alarm Module
        //!
        //! Contains the auto-generated alarms structs from Protobuf,
        //! for use when de/serializing records from the Controls Kafka instance.
        include!(concat!(env!("OUT_DIR"), "/common.alarm.rs"));
    }
}
mod google {
    pub mod protobuf {
        //! Generated Google Structs Module
        //!
        //! Contains the builtin structures (mainly [`Timestamp`]) provided by Google
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn should_create_key_from_string() {
        let result = phoebus::Key::from("command:some/path/here/MyDevice".to_string());
        assert_eq!(result.device, "MyDevice");
        assert_eq!(result.display_path, "some/path/here");
        assert_eq!(result.operation, phoebus::Operation::Command);

        let result = phoebus::Key::from("config:some/other/path/here/MyDevice2".to_string());
        assert_eq!(result.device, "MyDevice2");
        assert_eq!(result.display_path, "some/other/path/here");
        assert_eq!(result.operation, phoebus::Operation::Config);

        let result = phoebus::Key::from("state:some/path/here/MyDevice".to_string());
        assert_eq!(result.device, "MyDevice");
        assert_eq!(result.display_path, "some/path/here");
        assert_eq!(result.operation, phoebus::Operation::Other);
    }

    #[test]
    fn should_get_err_string_for_operation() {
        assert_eq!(
            "Cannot operate on type 'Other'",
            phoebus::Operation::get_err_string_for_other()
        );
    }

    #[test]
    fn should_get_operation_key_prefix() {
        assert_eq!("command", phoebus::Operation::Command.get_key_prefix());
        assert_eq!("config", phoebus::Operation::Config.get_key_prefix());
        assert_eq!("", phoebus::Operation::Other.get_key_prefix());
    }

    #[test]
    fn should_get_cached_state_from_alarm_status() {
        let status = alarm::Status {
            device: String::new(),
            source: alarm::status::Source::Analog as i32,
            state: alarm::status::State::Acknowledged as i32,
            severity: alarm::status::Severity::High as i32,
            acknowledgeable: false,
            time: None,
            epics_type: String::new(),
            user: String::new(),
            wake: None,
        };

        let result = CachedState::from(status);
        assert_eq!(result.state, alarm::status::State::Acknowledged);
        assert_eq!(result.wake, None);
    }

    #[test]
    fn should_create_and_clone_sync_config() {
        let controls_host = String::from("my controls host");
        let controls_topic = String::from("my controls topic");
        let phoebus_host = String::from("my phoebus host");
        let phoebus_topics = vec![String::from("topic1"), String::from("topic2")];

        let orig_config = SynchronizerConfig::new(
            controls_host.clone(),
            controls_topic.clone(),
            phoebus_host.clone(),
            phoebus_topics.clone(),
        );

        assert_eq!(controls_host, orig_config.controls_host);
        assert_eq!(controls_topic, orig_config.controls_topic);
        assert_eq!(phoebus_host, orig_config.phoebus_host);
        assert_eq!(phoebus_topics, orig_config.phoebus_topics);
        assert_eq!(1, Arc::strong_count(&orig_config.alarm_states));
        assert_eq!(1, Arc::strong_count(&orig_config.pv_metadata));

        let cloned_config = orig_config.clone();
        assert_eq!(orig_config, cloned_config);
    }
}

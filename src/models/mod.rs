pub mod phoebus {

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct Command {
        pub user: String,
        pub host: String,
        pub command: String,
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct Config {
        pub user: String,
        pub host: String,
        pub enabled: Option<String>, // This is either a time or a boolean - represented as a string to handle the ambiguity. Thanks EPICS.
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

    #[derive(Debug)]
    pub struct Key {
        pub operation: Operation,
        pub display_path: String,
        pub device: String,
    }
    impl From<String> for Key {
        fn from(value: String) -> Self {
            let (prefix, device) = value.rsplit_once("/").unwrap();
            let (command_str, display_path) = prefix.split_once(":").unwrap();
            Key {
                operation: Operation::from(command_str),
                display_path: display_path.to_owned(),
                device: device.to_owned(),
            }
        }
    }

    #[derive(Debug)]
    pub enum Operation {
        Command,
        Config,
        Other,
    }
    impl Operation {
        pub fn get_key_prefix(&self) -> &'static str {
            match self {
                Operation::Command => "command",
                Operation::Config => "config",
                Operation::Other => "",
            }
        }

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

    #[derive(Clone)]
    pub struct PvMetadata {
        pub config: Config,
        pub display_path: String,
        pub phoebus_topic: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct TitleDetails {
        pub title: String,
        pub details: String,
        pub delay: Option<String>,
    }
}

mod common {
    pub mod alarm {
        include!(concat!(env!("OUT_DIR"), "/common.alarm.rs"));
    }
}
pub use common::alarm;

mod google {
    pub mod protobuf {
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
    }
}
pub use google::protobuf as generated;

pub const ACK_COMMAND: &str = "acknowledge";

#[derive(Clone, Debug)]
pub struct CachedState {
    pub state: alarm::status::State,
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

use crate::{
    models::{
        ACK_COMMAND,
        alarm::{Status, status::State},
        phoebus::{Command, Config, Operation, PvMetadata},
    },
    utils::get_command_topic,
};
use chrono::{TimeZone, Utc};
use rust_pubsub_lib::Message;

const CONTROLS_HOST: &str = "Flutter Alarms App";

pub fn controls_to_phoebus(
    controls_alarm: &Status,
    operation: Operation,
    metadata: &PvMetadata,
) -> Result<(String, Message), String> {
    let topic = match operation {
        Operation::Command => get_command_topic(&metadata.phoebus_topic),
        Operation::Config => metadata.phoebus_topic.clone(),
        _ => return Err(Operation::get_err_string_for_other()),
    };
    let phoebus_message = transform(controls_alarm, operation, metadata)?;
    Ok((topic, phoebus_message))
}

fn get_phoebus_key(device: &String, operation: &Operation, metadata: &PvMetadata) -> String {
    format!(
        "{}:{}/{}",
        operation.get_key_prefix(),
        metadata.display_path,
        device
    )
}

fn transform(
    controls_alarm: &Status,
    operation: Operation,
    metadata: &PvMetadata,
) -> Result<Message, String> {
    let transformed_key = get_phoebus_key(&controls_alarm.device, &operation, metadata);
    let transformed_value = match operation {
        Operation::Command => serde_json::to_string(&Command {
            user: controls_alarm.user.clone(),
            host: CONTROLS_HOST.to_string(),
            command: ACK_COMMAND.to_string(),
        }),
        Operation::Config => {
            let enabled = Some(get_enabled_string(controls_alarm));
            let prev_config = metadata.config.clone();
            serde_json::to_string(&Config {
                user: controls_alarm.user.clone(),
                host: CONTROLS_HOST.to_string(),
                enabled,
                ..prev_config
            })
        }
        _ => return Err(Operation::get_err_string_for_other()),
    }
    .map_err(|e| format!("{e:?}"))?;
    Ok(Message {
        key: Some(transformed_key),
        value: transformed_value,
    })
}

fn get_enabled_string(controls_alarm: &Status) -> String {
    if controls_alarm.state() == State::Bypassed {
        match controls_alarm
            .wake
            .and_then(|t| Utc.timestamp_opt(t.seconds, t.nanos as u32).single())
        {
            Some(dt) => dt.to_rfc3339(),
            None => "false".to_string(),
        }
    } else {
        "true".to_string()
    }
}

use crate::generated::{
    common::alarm::{
        Status as ControlsAlarm,
        status::{Severity, Source, State},
    },
    google::protobuf::Timestamp,
};
use rust_pubsub_lib::Message;

pub fn controls_to_phoebus(msg: Message) -> Result<Message, serde_json::Error> {
    let controls_alarm: ControlsAlarm = serde_json::from_str(&msg.value)?;
    let phoebus_alarm = PhoebusAlarm::from(controls_alarm);
    Ok(Message {
        key: msg.key,
        value: serde_json::to_string(&phoebus_alarm)?,
    })
}

pub fn phoebus_to_controls(msg: Message) -> Result<Message, serde_json::Error> {
    let phoebus_alarm: PhoebusAlarm = serde_json::from_str(&msg.value)?;
    let controls_alarm = ControlsAlarm::from(phoebus_alarm);
    Ok(Message {
        key: msg.key,
        value: serde_json::to_string(&controls_alarm)?,
    })
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PhoebusAlarm;

impl From<ControlsAlarm> for PhoebusAlarm {
    fn from(_controls_alarm: ControlsAlarm) -> Self {
        // Map fields from ControlsAlarm to PhoebusAlarm here.
        // This is a placeholder implementation and should be replaced with actual mapping logic.
        PhoebusAlarm
    }
}

impl From<PhoebusAlarm> for ControlsAlarm {
    fn from(_phoebus_alarm: PhoebusAlarm) -> ControlsAlarm {
        // Map fields from PhoebusAlarm to ControlsAlarm here.
        // This is a placeholder implementation and should be replaced with actual mapping logic.
        ControlsAlarm {
            acknowledgeable: false,
            device: String::new(),
            epics_type: String::new(),
            source: Source::Epics as i32,
            severity: Severity::Unknown as i32,
            state: State::Unknown as i32,
            time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            user: String::new(),
            wake: None,
        }
    }
}

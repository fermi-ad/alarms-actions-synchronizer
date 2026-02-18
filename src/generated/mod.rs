pub mod common {
    pub mod alarm {
        include!(concat!(env!("OUT_DIR"), "/common.alarm.rs"));
    }
}

pub mod google {
    pub mod protobuf {
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
    }
}

use std::{env, io::Error};

fn main() -> Result<(), Error> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path().expect("failed to find protoc");
    unsafe {
        env::set_var("PROTOC", protoc_path);
    }
    let mut config = prost_build::Config::new();
    config
        .type_attribute(
            ".google.protobuf",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_well_known_types()
        .type_attribute(
            ".common.alarm",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(
            &["extern/interfaces/proto/controls/common/v1/alarm.proto"],
            &["extern/interfaces/proto"],
        )
}

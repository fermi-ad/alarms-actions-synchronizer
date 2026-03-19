use std::{env, io::Error};

fn main() -> Result<(), Error> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path().expect("failed to find protoc");
    unsafe {
        env::set_var("PROTOC", protoc_path);
    }
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .build_transport(false)
        .type_attribute(
            ".google.protobuf",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_well_known_types(true)
        .type_attribute(
            ".common.alarm",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(
            &["extern/interfaces/proto/controls/common/v1/alarm.proto"],
            &["extern/interfaces/proto/controls/common/v1"],
        )?;

    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &["extern/interfaces/proto/controls/service/grpc-alarm-commands/v1/alarm_commands.proto"], 
            &["extern/interfaces/proto/controls/service/grpc-alarm-commands/v1"]
        )
}

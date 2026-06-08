use rust_grpc_lib::build_support::{Config, generate_protos};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::new()
        .type_attribute(
            ".google.protobuf",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            ".services.alarm_commands",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            ".common.alarm",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        );

    generate_protos(config)?;
    Ok(())
}

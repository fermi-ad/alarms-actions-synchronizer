fn main() -> Result<(), Box<dyn std::error::Error>> {
    rust_grpc_lib::build_support::generate_protos()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=opentelemetry/");
    
    // First try basic compilation without extern_path to see if it works
    prost_build::compile_protos(
        &[
            "opentelemetry/proto/common/v1/common.proto",
            "opentelemetry/proto/resource/v1/resource.proto", 
            "opentelemetry/proto/trace/v1/trace.proto",
        ],
        &["."],
    )?;

    println!("Protobuf compilation completed");
    Ok(())
}
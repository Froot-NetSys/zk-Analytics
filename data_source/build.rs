// Compile the minimal Trillian log proto into a tonic gRPC client, but only when
// the `trillian` feature is enabled. The block is gated with `cfg(feature)` so
// that without the feature the optional `tonic-build` dependency is never
// referenced (and never compiled), keeping default builds free of any
// tonic/protoc toolchain requirement.
fn main() {
    println!("cargo:rerun-if-changed=proto/trillian_log.proto");
    #[cfg(feature = "trillian")]
    {
        tonic_build::configure()
            .build_server(false)
            .compile_protos(&["proto/trillian_log.proto"], &["proto"])
            .expect("failed to compile proto/trillian_log.proto");
    }
}

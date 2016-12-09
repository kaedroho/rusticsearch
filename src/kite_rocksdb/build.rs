extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .file("compact_segment.capnp")
        .run()
        .expect("compiling schema");
}

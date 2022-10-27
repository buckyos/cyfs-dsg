extern crate protoc_rust;

use std::io::Write;

static MOD_PROTOS_RS:&str = r#"
mod contracts;
mod proof;
mod cache;
pub use contracts::*;
pub use proof::*;
pub use cache::*;
"#;

fn gen_protos() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let mut gen = protoc_rust::Codegen::new();
    gen.input("protos/contracts.proto")
    .input("protos/proof.proto")
    .input("protos/cache.proto")
    .protoc_path(protoc_bin_vendored::protoc_bin_path().unwrap())
        .out_dir(&out_dir)
        .customize(protoc_rust::Customize {
            expose_fields: Some(true),
            ..Default::default()
        });

    gen.run().expect("protoc error!");

    std::fs::File::create(out_dir + "/mod.rs").expect("write protos mod error")
        .write_all(MOD_PROTOS_RS.as_ref()).expect("write protos mod error");
}

fn main() {
    println!("cargo:rerun-if-changed=protos");
    gen_protos();
}

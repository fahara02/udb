fn main() {
    println!("cargo:rustc-check-cfg=cfg(udb_portable)");
    println!("cargo:rustc-cfg=udb_portable");
}

// Crate lint gates. Keep these at warn until the public surface is explicit
// enough to promote unused/dead code to deny.
#![warn(unused)]
#![warn(dead_code)]

mod cli;

fn main() {
    cli::run();
}

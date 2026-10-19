// Required by the #[copper_runtime] proc-macro: it reads LOG_INDEX_DIR to
// locate the string-interning index it writes at compile time.
fn main() {
    println!(
        "cargo:rustc-env=LOG_INDEX_DIR={}",
        std::env::var("OUT_DIR").unwrap()
    );
}

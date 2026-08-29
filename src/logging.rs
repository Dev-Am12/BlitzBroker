//! A minimal hand-rolled leveled logger — see STDLIB.md (`log`/`tracing`
//! substitution). Writes timestamped lines to stdout/stderr.
//!
//! Owner: Role D, but kept minimally functional here so Role A and
//! connection.rs have something to call immediately. Extend freely
//! (levels filtering, structured fields, etc.) without breaking this
//! call signature.

use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn info(msg: &str) {
    println!("[{}] INFO  {msg}", timestamp());
}

pub fn warn(msg: &str) {
    println!("[{}] WARN  {msg}", timestamp());
}

pub fn error(msg: &str) {
    eprintln!("[{}] ERROR {msg}", timestamp());
}

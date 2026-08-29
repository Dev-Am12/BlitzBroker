//! BlitzBroker entry point: parses CLI args, starts the broker actor
//! thread, and runs the TCP accept loop, spawning a connection handler
//! per client. See PLAN.md for architecture and DECISIONS.md for the
//! concurrency-model reasoning.

mod broker;
mod connection;
mod error;
mod logging;
mod protocol;
mod queue;

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;

struct Config {
    host: String,
    port: u16,
}

/// Hand-rolled CLI parsing — see STDLIB.md (`clap` substitution). Accepts
/// `--host <addr>` and `--port <port>`; both optional with sane
/// defaults.
fn parse_args() -> Config {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 1883; // MQTT's conventional default port

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                if let Some(v) = args.get(i + 1) {
                    host = v.clone();
                    i += 1;
                }
            }
            "--port" => {
                if let Some(v) = args.get(i + 1) {
                    if let Ok(p) = v.parse() {
                        port = p;
                    }
                    i += 1;
                }
            }
            other => {
                logging::warn(&format!("ignoring unrecognized argument: {other}"));
            }
        }
        i += 1;
    }

    Config { host, port }
}

fn main() {
    let config = parse_args();
    let addr = format!("{}:{}", config.host, config.port);

    let (broker_tx, broker_rx) = mpsc::channel::<broker::BrokerMessage>();

    let broker_handle = thread::spawn(move || {
        broker::run_broker(broker_rx);
    });

    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            logging::error(&format!("failed to bind {addr}: {e}"));
            std::process::exit(1);
        }
    };
    logging::info(&format!("BlitzBroker listening on {addr}"));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let tx = broker_tx.clone();
                thread::spawn(move || {
                    connection::handle_connection(stream, tx);
                });
            }
            Err(e) => {
                logging::warn(&format!("accept error: {e}"));
            }
        }
    }

    // Only reached if the listener stops iterating (shouldn't happen in
    // normal operation). Drop the sender so the broker thread's channel
    // closes and it can exit cleanly.
    drop(broker_tx);
    let _ = broker_handle.join();
}

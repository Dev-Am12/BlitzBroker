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
use std::thread;

use broker::{spawn_sharded_broker, NUM_BROKER_SHARDS};

struct Config {
    host: String,
    port: u16,
    /// Number of broker shards. Defaults to NUM_BROKER_SHARDS (4).
    /// Exposed via --shards <N> to allow before/after shard-count comparison
    /// without maintaining separate builds — see DECISIONS.md #11.
    num_shards: usize,
}

/// Hand-rolled CLI parsing — see STDLIB.md (`clap` substitution). Accepts
/// `--host <addr>`, `--port <port>`, and `--shards <N>`; all optional with
/// sane defaults.
fn parse_args() -> Config {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 1883; // MQTT's conventional default port
    let mut num_shards: usize = NUM_BROKER_SHARDS;

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
            "--shards" => {
                if let Some(v) = args.get(i + 1) {
                    match v.parse::<usize>() {
                        Ok(n) if n >= 1 => num_shards = n,
                        _ => logging::warn(&format!(
                            "--shards must be a positive integer, ignoring value {:?}",
                            v
                        )),
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

    Config {
        host,
        port,
        num_shards,
    }
}

fn main() {
    let config = parse_args();
    let addr = format!("{}:{}", config.host, config.port);

    // Spawn N independent broker threads (one per shard). Each owns a
    // disjoint subset of topics — see DECISIONS.md #1 and PLAN.md §4 item 3.
    let broker = spawn_sharded_broker(config.num_shards);
    logging::info(&format!(
        "BlitzBroker: {} broker shard(s) active",
        config.num_shards
    ));

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
                // ShardedBroker is cheaply cloneable (Arc<Vec<Sender>>).
                let broker = broker.clone();
                thread::spawn(move || {
                    connection::handle_connection(stream, broker);
                });
            }
            Err(e) => {
                logging::warn(&format!("accept error: {e}"));
            }
        }
    }

    // Only reached if the listener stops iterating (shouldn't happen in
    // normal operation). Dropping the ShardedBroker closes all shard
    // channels, allowing each broker thread to exit cleanly.
    drop(broker);
}

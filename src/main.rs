extern crate kite;
extern crate kite_rocksdb;
extern crate chrono;
#[macro_use]
extern crate router;
extern crate url;
#[macro_use]
extern crate log;
#[macro_use(o, b)]
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate maplit;
extern crate unicode_segmentation;
extern crate uuid;
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate atomicwrites;
extern crate fnv;

pub mod analysis;
pub mod query_parser;
pub mod mapping;
pub mod document;
pub mod index;
pub mod cluster;
pub mod system;
mod api;
mod logger;

use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::panic;

use slog::Logger;

use system::System;


const VERSION: &'static str = env!("CARGO_PKG_VERSION");


fn main() {
    let log = Logger::new_root(o!());
    log.set_drain(slog_term::async_stderr());

    log.info("[sys] starting rusticsearch", b!("version" => VERSION));

    logger::init().unwrap();

    let system = Arc::new(System::new(log, Path::new("data/").to_path_buf()));

    system.log.info("[sys] loading indices", b!());
    system.load_indices();

    {
        let system = system.clone();
        thread::spawn(move || {
            loop {
                {
                    let cluster_metadata = system.metadata.read().unwrap();
                    for index in cluster_metadata.indices.values() {
                        let result = panic::catch_unwind(|| {
                            index.run_maintenance_task().unwrap();
                        });

                        if let Err(error) = result {
                            system.log.error("[sys] index maintenance task panicked", b!("index" => index.canonical_name(), "error" => format!("{:?}", error)));
                        }
                    }
                }

                thread::sleep(Duration::new(1, 0));
            }
        });
    }

    system.log.info("[sys] starting api server", b!());
    api::api_main(system);
}

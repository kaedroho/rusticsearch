extern crate chrono;
#[macro_use]
extern crate router;
extern crate url;
#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;
#[macro_use]
extern crate maplit;
extern crate unicode_segmentation;
extern crate uuid;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate atomicwrites;
extern crate fnv;
#[macro_use]
extern crate bitflags;
extern crate roaring;
extern crate byteorder;
extern crate rocksdb;

pub mod search;
pub mod analysis;
pub mod query_parser;
pub mod mapping;
pub mod document;
pub mod index;
pub mod cluster;
pub mod system;
mod api;

use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::panic;

use slog::Drain;

use system::System;


const VERSION: &'static str = env!("CARGO_PKG_VERSION");


fn main() {
    // Setup logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());

    info!(log, "starting rusticsearch"; "version" => VERSION);

    let system = Arc::new(System::new(log, Path::new("data/").to_path_buf()));

    info!(system.log, "loading indices");
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
                            error!(system.log, "index maintenance task panicked"; "index" => index.canonical_name(), "error" => format!("{:?}", error));
                        }
                    }
                }

                thread::sleep(Duration::new(1, 0));
            }
        });
    }

    info!(system.log, "starting api server");
    api::api_main(system);
}

extern crate abra;
extern crate chrono;
#[macro_use]
extern crate router;
extern crate url;
extern crate rustc_serialize;
#[macro_use]
extern crate log;
#[macro_use(o, b)]
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate maplit;

pub mod query_parser;
pub mod mapping;
pub mod document;
pub mod index;
pub mod system;
mod api;
mod logger;

use std::path::Path;
use std::sync::Arc;

use slog::Logger;

use system::System;


const VERSION: &'static str = "0.1a0";


fn main() {
    let log = Logger::new_root(o!());
    log.set_drain(slog_term::async_stderr());

    log.info("[sys] starting rusticsearch", b!("version" => VERSION));

    logger::init().unwrap();

    let system = Arc::new(System::new(log, Path::new("data/").to_path_buf()));

    system.log.info("[sys] loading indices", b!());
    system.load_indices();

    system.log.info("[sys] starting api server", b!());
    api::api_main(system);
}

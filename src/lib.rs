#![feature(nll)]

#[macro_use]
extern crate derive_builder;
extern crate failure;
extern crate itertools;
#[macro_use]
extern crate log;
extern crate io_at;
extern crate log4rs;
extern crate integer_encoding;
extern crate regex;
#[macro_use]
extern crate lazy_static;

mod core;
mod segment;
mod store;
mod active_data;
mod keys_iterator;
mod hint;

pub use core::{Config, ConfigBuilder};
pub use core::Bitcask;
use std::sync::{Once, ONCE_INIT};
pub use store::{escape_tombstone, unescape_tombstone};

pub use keys_iterator::StoreKeys;

static INIT: Once = ONCE_INIT;

/// Setup function that is only run once, even if called multiple times.
pub fn setup() {
    INIT.call_once(|| {
        log4rs::init_file("log4rs.yml", Default::default()).unwrap();
    });
}

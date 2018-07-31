#![feature(nll)]

#[macro_use]
extern crate derive_builder;
extern crate failure;
extern crate itertools;
#[macro_use]
extern crate log;
extern crate integer_encoding;
extern crate io_at;
extern crate log4rs;
extern crate regex;
#[macro_use]
extern crate lazy_static;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;

mod core;
mod hint;
mod keys_iterator;
mod segment;
mod store;

pub use core::Bitcask;
pub use core::{Config, ConfigBuilder};
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

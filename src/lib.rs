#![feature(nll)]
#![feature(test)]

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
extern crate test;
extern crate twox_hash;

mod core;
mod hint;
mod keys_iterator;
mod segment;
mod store;

pub use core::Bitcask;
pub use core::{Config, ConfigBuilder};

pub use keys_iterator::StoreKeys;

use std::sync::{Once, ONCE_INIT};

static INIT: Once = ONCE_INIT;

/// Read config file 'log4rs.yml'
pub fn setup(path: &str) {
    INIT.call_once(|| {
        log4rs::init_file(path, Default::default()).expect("log4rs.yml not found");
    });
}

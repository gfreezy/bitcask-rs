#![feature(nll)]

extern crate byteorder;
#[macro_use]
extern crate derive_builder;
extern crate failure;
extern crate itertools;
#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate io_at;

pub use core::{Config, ConfigBuilder};
pub use core::Bitcask;

mod core;
mod segment;
mod store;

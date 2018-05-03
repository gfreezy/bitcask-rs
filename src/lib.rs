#![feature(nll)]

extern crate byteorder;
#[macro_use]
extern crate derive_builder;
extern crate failure;
#[macro_use]
extern crate log;
extern crate simple_logger;

pub use core::Bitcask;
pub use core::Config;

mod core;
mod segment;
mod store;
mod entry;


#![feature(test)]

extern crate bitcask_rs;
extern crate itertools;
extern crate rand;
extern crate test;

use rand::distributions::Alphanumeric;
use rand::Rng;
use std::fs;
use std::path::PathBuf;
use test::Bencher;

#[bench]
fn get_latency(b: &mut Bencher) {
    let id: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .collect();
    let path = format!("target/benches/bench-{}.db", id);
    let config = bitcask_rs::ConfigBuilder::default()
        .path(PathBuf::from(&path))
        .max_size_per_segment(50 * 1024 * 1024)
        .build()
        .unwrap();
    let mut bitcask = bitcask_rs::Bitcask::new(config);
    let key = vec![1u8; 512];
    let vec = vec![1u8; 4096];

    let set_ret = bitcask.set(key.clone(), vec.clone());
    assert!(set_ret.is_ok());

    b.iter(|| bitcask.get(&key).unwrap());

    fs::remove_dir_all(path).unwrap();
}

#[bench]
fn put_latency(b: &mut Bencher) {
    let id: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .collect();
    let path = format!("bench-{}.db", id);
    let config = bitcask_rs::ConfigBuilder::default()
        .path(PathBuf::from(&path))
        .max_size_per_segment(50 * 1024 * 1024)
        .build()
        .unwrap();
    let mut bitcask = bitcask_rs::Bitcask::new(config);
    let key = vec![1u8; 512];
    let vec = vec![1u8; 4096];

    let set_ret = bitcask.set(key.clone(), vec.clone());
    assert!(set_ret.is_ok());

    b.iter(|| bitcask.set(key.clone(), vec.clone()).unwrap());

    fs::remove_dir_all(path).unwrap();
}

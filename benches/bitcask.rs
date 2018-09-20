#![feature(test)]

extern crate bitcask_rs;
extern crate test;
extern crate rand;
extern crate itertools;

use test::Bencher;
use std::path::PathBuf;
use std::fs;
use rand::Rng;


#[bench]
fn get_latency(b: &mut Bencher) {
    let id: String = rand::thread_rng().gen_ascii_chars().take(16).collect();
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

    b.bytes = vec.len() as u64;
    b.iter(|| bitcask.get(&key).unwrap());

    fs::remove_dir_all(path).unwrap();
}

#[bench]
fn put_latency(b: &mut Bencher) {
    let id: String = rand::thread_rng().gen_ascii_chars().take(16).collect();
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

    b.bytes = vec.len() as u64;
    b.iter(|| bitcask.set(key.clone(), vec.clone()).unwrap());

    fs::remove_dir_all(path).unwrap();
}

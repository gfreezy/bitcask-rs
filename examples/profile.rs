extern crate bitcask_rs;
extern crate rand;
extern crate itertools;

use std::path::PathBuf;
use std::fs;
use rand::Rng;


fn main() {
    let id: String = rand::thread_rng().gen_ascii_chars().take(16).collect();
    let path = format!("target/bench-{}.db", id);
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

    for i in 0..100000000 {
        bitcask.get(&key).unwrap();
    }
    fs::remove_dir_all(path).unwrap();
}

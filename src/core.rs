use failure::Error;
use keys_iterator::StoreKeys;
use serde_yaml;
use std;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use store::Store;

pub type Key = String;
pub type Value = Vec<u8>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Builder, Clone)]
#[builder(default)]
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub path: PathBuf,
    pub max_size_per_segment: u64,
    pub max_file_id: u64,
    pub min_merge_file_id: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            path: std::env::current_dir().expect("get current dir"),
            max_size_per_segment: 100_000_000,
            max_file_id: 1_000_000_000,
            min_merge_file_id: 100_000_000_000,
        }
    }
}

impl Config {
    pub fn new<T: AsRef<Path>>(config_path: T) -> Self {
        let mut file = File::open(config_path).expect("open config path");
        let config: Config = serde_yaml::from_reader(&mut file).expect("deserialize config file");
        return config;
    }
}

pub struct Bitcask {
    config: Arc<Config>,
    store: Arc<Store>,
}

impl Bitcask {
    pub fn new(config: Config) -> Self {
        let arc_config = Arc::new(config);

        Bitcask {
            store: Arc::new(Store::new(arc_config.clone())),
            config: arc_config,
        }
    }

    pub fn open(config: Config) -> Self {
        let arc_config = Arc::new(config);
        Bitcask {
            store: Arc::new(Store::open(arc_config.clone())),
            config: arc_config,
        }
    }

    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        self.store.get(key)
    }

    pub fn set(&mut self, key: Key, value: Value) -> Result<()> {
        self.store.insert(key, value)
    }

    pub fn delete(&mut self, key: Key) -> Result<()> {
        self.store.delete(key)
    }

    pub fn exists(&self, key: Key) -> Result<bool> {
        self.store.exists(key)
    }

    pub fn merge(&mut self, since: Option<u64>) -> Result<()> {
        let file_ids = if let Some(file_id) = since {
            self.store.prepare_merging_since(file_id)
        } else {
            self.store.prepare_full_merging()
        };
        debug!(target: "core::merge", "file_ids: {:?}", file_ids);
        let ret = self.store.merge(&file_ids)?;
        self.store.finish_merging(ret)
    }

    pub fn keys(&self) -> StoreKeys {
        self.store.keys()
    }
}

impl Clone for Bitcask {
    fn clone(&self) -> Self {
        Bitcask {
            config: self.config.clone(),
            store: self.store.clone(),
        }
    }
}

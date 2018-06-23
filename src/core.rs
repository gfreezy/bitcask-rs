use failure::Error;
use std;
use std::path::PathBuf;
use store::Store;
use std::sync::Arc;
use keys_iterator::StoreKeys;


pub type Key = String;
pub type Value = Vec<u8>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Default, Builder, Clone)]
pub struct Config {
    pub path: PathBuf,
}

pub struct Bitcask {
    config: Arc<Config>,
    store: Arc<Store>,
}

impl Bitcask {
    pub fn new(config: Config) -> Self {
        Bitcask {
            store: Arc::new(Store::new(&config.path)),
            config: Arc::new(config),
        }
    }

    pub fn open(config: Config) -> Self {
        Bitcask {
            store: Arc::new(Store::open(&config.path)),
            config: Arc::new(config),
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
        let ret = self.store.merge(file_ids)?;
        self.store.finish_merging(ret)
    }

    pub fn keys<'a>(&'a self) -> StoreKeys<'a> {
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

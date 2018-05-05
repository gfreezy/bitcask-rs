use failure::Error;
use segment::Offset;
use std;
use std::collections::HashMap;
use std::path::PathBuf;
use store::Store;

pub type Key = String;
pub type Value = Vec<u8>;
pub type Result<T> = std::result::Result<T, Error>;

pub const TOMBSTONE: &str = "<<>>";

#[derive(Default, Builder, Clone)]
pub struct Config {
    pub wal_path: PathBuf,
}

pub struct Bitcask {
    hashmap: HashMap<Key, Offset>,
    store: Store,
    config: Config,
}

impl Bitcask {
    pub fn new(config: Config) -> Self {
        Bitcask {
            hashmap: HashMap::new(),
            store: Store::new(&config.wal_path, 0),
            config,
        }
    }

    pub fn open(config: Config) -> Self {
        let mut store = Store::open(&config.wal_path);
        let hashmap = store.build_hashmap().expect("build hashmap from segment files");
        Bitcask {
            hashmap,
            store,
            config,
        }
    }

    pub fn get(&mut self, key: Key) -> Result<Option<Value>> {
        match self.hashmap.get(&key) {
            None => Ok(None),
            Some(offset) => {
                debug!("Bitcask.get key: {}, offset: {}", &key, offset);
                self.store.get(*offset)
            }
        }
    }

    pub fn set(&mut self, key: Key, value: Value) -> Result<()> {
        let offset = self.store.insert(key.clone(), value)?;
        let _ = self.hashmap.insert(key.clone(), offset);
        debug!("Bitcask.set key: {}, offset: {}", &key, offset);
        Ok(())
    }

    pub fn delete(&mut self, key: Key) -> Result<()> {
        match self.hashmap.get(&key) {
            Some(_) => {
                self.set(key.clone(), TOMBSTONE.as_bytes().to_vec())?;
                self.hashmap.remove(&key);
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn exists(&self, key: Key) -> Result<bool> {
        Ok(self.hashmap.contains_key(&key))
    }

    pub fn compact(&mut self) -> Result<()> {
        let (new_store, new_hashmap) = self.store.compact(&self.hashmap)?;
        self.store = new_store;
        self.hashmap = new_hashmap;
        Ok(())
    }
}

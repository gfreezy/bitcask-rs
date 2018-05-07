use failure::{Error, err_msg};
use segment::Offset;
use std;
use std::collections::HashMap;
use std::path::PathBuf;
use store::Store;
use std::sync::RwLock;
use std::sync::Arc;

pub type Key = String;
pub type Value = Vec<u8>;
pub type Result<T> = std::result::Result<T, Error>;

pub const TOMBSTONE: &str = "<<>>";

#[derive(Default, Builder, Clone)]
pub struct Config {
    pub wal_path: PathBuf,
}

pub struct Bitcask {
    config: Config,
    core: Arc<RwLock<Core>>,
}

impl Bitcask {
    pub fn new(config: Config) -> Self {
        Bitcask {
            core: Arc::new(RwLock::new(Core::new(&config.wal_path))),
            config,
        }
    }

    pub fn open(config: Config) -> Self {
        Bitcask {
            core: Arc::new(RwLock::new(Core::open(&config.wal_path))),
            config,
        }
    }

    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        match self.core.read() {
            Ok(core) => core.get(key),
            Err(_) => Err(err_msg("lock get"))
        }
    }

    pub fn set(&mut self, key: Key, value: Value) -> Result<()> {
        match self.core.write() {
            Ok(mut core) => core.set(key, value),
            Err(_) => Err(err_msg("lock set"))
        }
    }

    pub fn delete(&mut self, key: Key) -> Result<()> {
        match self.core.write() {
            Ok(mut core) => core.delete(key),
            Err(_) => Err(err_msg("lock delete"))
        }
    }

    pub fn exists(&self, key: Key) -> Result<bool> {
        match self.core.read() {
            Ok(core) => core.exists(key),
            Err(_) => Err(err_msg("lock read"))
        }
    }

    pub fn compact(&mut self) -> Result<()> {
        match self.core.write() {
            Ok(mut core) => {
                core.compact()
            },
            Err(_) => Err(err_msg("lock compact"))
        }
    }
}

impl Clone for Bitcask {
    fn clone(&self) -> Self {
        Bitcask {
            config: self.config.clone(),
            core: self.core.clone()
        }
    }
}


pub struct Core {
    hashmap: HashMap<Key, Offset>,
    store: Store,
}

impl Core {
    pub fn new(path: &PathBuf) -> Self {
        Core {
            hashmap: HashMap::new(),
            store: Store::new(path, 0),
        }
    }

    pub fn open(path: &PathBuf) -> Self {
        let store = Store::open(path);
        let hashmap = store.build_hashmap().expect("build hashmap from segment files");
        Core {
            hashmap,
            store,
        }
    }

    pub fn get(&self, key: Key) -> Result<Option<Value>> {
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

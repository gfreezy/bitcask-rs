use ::core::{Key, Result, Value, Config};
use ::segment::{Offset, Segment};
use std::collections::HashMap;
use std::fs::{read_dir, rename};
use std::path::PathBuf;
use std::mem;
use std::sync::{RwLock, Arc};
use keys_iterator::StoreKeys;

pub const TOMBSTONE: &str = "<<>>";

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Position {
    pub file_id: u64,
    pub offset: Offset,
}

impl Position {
    pub fn not_exist() -> Position {
        Position {
            file_id: 0,
            offset: 0,
        }
    }
}

#[derive(Default)]
pub struct MergeResult {
    merged_hashmap: HashMap<Key, Position>,
    new_file_ids: Vec<u64>,
    to_remove_file_ids: Vec<u64>,
}

pub struct ActiveData {
    active_segment: Segment,
    active_hashmap: HashMap<Key, Position>,
    pending_segments: HashMap<u64, Segment>,
    pending_hashmap: HashMap<Key, Position>,
    config: Arc<Config>,
}

impl ActiveData {
    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        if let Some(pos) = self.active_hashmap.get(&key) {
            return self.active_segment.get(pos.offset);
        }

        if let Some(pos) = self.pending_hashmap.get(&key) {
            return self.pending_segments.get(&pos.file_id).map_or(Ok(None), |s| s.get(pos.offset))
        }
        Ok(None)
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<bool> {
        let active_segment = &mut self.active_segment;
        let offset = active_segment.insert(key.clone(), value)?;
        let file_id = active_segment.file_id;
        let active_hashmap = &mut self.active_hashmap;
        active_hashmap.insert(key, Position { offset, file_id });

        Ok(active_segment.size >= self.config.max_size_per_segment)
    }

    pub fn rotate(&mut self, mut segment: Segment) {
        let mut new_active_hashmap = HashMap::with_capacity(100);
        mem::swap(&mut new_active_hashmap, &mut self.active_hashmap);
        self.pending_hashmap.extend(new_active_hashmap);

        mem::swap(&mut self.active_segment, &mut segment);
        self.pending_segments.insert(segment.file_id, segment);
    }

    pub fn delete(&mut self, key: Key) -> Result<()> {
        let hashmap = &mut self.active_hashmap;
        match hashmap.get(&key) {
            Some(_) => {
                hashmap.insert(key.clone(), Position::not_exist());
                self.active_segment.insert(key, TOMBSTONE.as_bytes().to_vec())?;
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn exists(&self, key: Key) -> Result<bool> {
        Ok(match self.active_hashmap.get(&key) {
            Some(v) => *v != Position::not_exist(),
            None => {
                match self.pending_hashmap.get(&key) {
                    None => false,
                    Some(v) => *v != Position::not_exist()
                }
            }
        })
    }

    pub fn keys<'a>(&'a self) -> Box<Iterator<Item = &'a String> + 'a> {
        Box::new(self.active_hashmap.keys().chain(self.pending_hashmap.keys()))
    }
}


pub struct OlderData {
    segments: HashMap<u64, Segment>,
    hashmap: HashMap<Key, Position>,
    config: Arc<Config>,
}


impl OlderData {
    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        if let Some(pos) = self.hashmap.get(&key) {
            return self.segments.get(&pos.file_id).map_or(Ok(None), |s| s.get(pos.offset))
        }
        Ok(None)
    }

    pub fn add_segment(&mut self, segment: Segment) {
        self.segments.insert(segment.file_id, segment);
    }

    fn remove_segment(&mut self, file_id: u64) -> Result<()> {
        let seg = self.segments.remove(&file_id);
        if let Some(mut seg) = seg {
            seg.destroy()?;
        }
        Ok(())
    }

    pub fn keys<'a>(&'a self) -> Box<Iterator<Item = &'a String> + 'a> {
        Box::new(self.hashmap.keys())
    }
}


pub struct Store {
    path: PathBuf,
    next_file_id: RwLock<u64>,
    older_data: RwLock<OlderData>,
    active_data: RwLock<ActiveData>,
    config: Arc<Config>,
}


impl Store {
    pub fn new(config: Arc<Config>) -> Self {
        let path = &config.path;
        Store {
            path: config.path.clone(),
            next_file_id: RwLock::new(1),
            older_data: RwLock::new(OlderData {
                segments: HashMap::new(),
                hashmap: HashMap::new(),
                config: config.clone(),
            }),
            active_data: RwLock::new(ActiveData {
                active_segment: Segment::new(0, path),
                active_hashmap: HashMap::with_capacity(100),
                pending_segments: HashMap::with_capacity(10),
                pending_hashmap: HashMap::with_capacity(100),
                config: config.clone(),
            }),
            config: config.clone(),
        }
    }

    pub fn open(config: Arc<Config>) -> Self {
        let path = &config.path;
        let mut hashmap = HashMap::with_capacity(100);
        let mut segments = HashMap::with_capacity(100);
        let mut max_file_id = 0;
        for entry in read_dir(path).expect("read segments dir") {
            let entry = entry.expect("read path entry");
            let segment_path = entry.path();
            let file_id = segment_path.file_stem().expect("get file id").to_str().expect("to string").parse::<u64>().expect("parse int");
            max_file_id = max_file_id.max(file_id);
            let seg = Segment::open(file_id, path);
            for entry_result in seg.iter() {
                let entry = entry_result.expect("get entry");
                hashmap.insert(entry.key.clone(), Position { file_id, offset: entry.offset });
            }
            debug!(target: "bitcask::store::open", "add segment: {:?}", file_id);
            segments.insert(file_id, seg);
        };
        Store {
            path: path.clone(),
            next_file_id: RwLock::new(max_file_id + 2),
            older_data: RwLock::new(OlderData {
                segments,
                hashmap,
                config: config.clone(),
            }),
            active_data: RwLock::new(ActiveData {
                active_segment: Segment::new(max_file_id + 1, path),
                active_hashmap: HashMap::with_capacity(100),
                pending_segments: HashMap::with_capacity(10),
                pending_hashmap: HashMap::with_capacity(100),
                config: config.clone(),
            }),
            config: config.clone(),
        }
    }

    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        let ret = self.active_data.read().expect("lock read").get(key.clone())?;
        if let Some(v) = ret {
            if v.as_slice() == TOMBSTONE.as_bytes() {
                return Ok(None);
            }
            return Ok(Some(v));
        }

        let ret = self.older_data.read().expect("lock read").get(key)?;
        if let Some(v) = ret {
            if v.as_slice() == TOMBSTONE.as_bytes() {
                return Ok(None);
            }
            return Ok(Some(v));
        }

        Ok(None)
    }

    pub fn insert(&self, key: Key, value: Value) -> Result<()> {
        let mut active_data = self.active_data.write().expect("lock write");
        let to_rotate = active_data.insert(key, value)?;
        if to_rotate {
            let mut next_file_id = self.next_file_id.write().expect("lock write");
            let file_id = *next_file_id;
            *next_file_id += 1;
            active_data.rotate(Segment::new(file_id, &self.path));
            assert!(file_id < self.config.max_file_id);
        }

        if !active_data.pending_hashmap.is_empty() {
            if let Ok(mut older_data) = self.older_data.try_write() {
                let mut pending_segments = HashMap::new();
                let mut pending_hashmap = HashMap::new();
                mem::swap(&mut active_data.pending_segments, &mut pending_segments);
                mem::swap(&mut active_data.pending_hashmap, &mut pending_hashmap);

                older_data.segments.extend(pending_segments);
                older_data.hashmap.extend(pending_hashmap);
            }
        }

        Ok(())
    }

    pub fn delete(&self, key: Key) -> Result<()> {
        self.insert(key, TOMBSTONE.as_bytes().to_vec())
    }

    pub fn exists(&self, key: Key) -> Result<bool> {
        Ok(match self.get(key)? {
            None => false,
            Some(v) => v.as_slice() != TOMBSTONE.as_bytes()
        })
    }

    fn remove_segment(&self, file_id: u64) -> Result<()> {
        self.older_data.write().expect("lock write").remove_segment(file_id)
    }

    fn rename_segment(&self, from: u64, to: u64) -> Result<()> {
        rename(Segment::get_path(from, &self.path), Segment::get_path(to, &self.path))?;
        Ok(())
    }

    pub fn keys(&self) ->  StoreKeys {
        StoreKeys {
            active_data_guard: self.active_data.read().expect("lock read"),
            older_data_guard: self.older_data.read().expect("lock read"),
        }
    }

    pub fn prepare_full_merging(&self) -> Vec<u64> {
        self.older_data.read().expect("lock read").segments.keys().cloned().map(|s| s).collect()
    }

    pub fn prepare_merging_since(&self, file_id: u64) -> Vec<u64> {
        self.older_data.read().expect("lock read").segments.keys().cloned().map(|s| s).filter(|s| *s >= file_id).collect()
    }

    pub fn merge(&self, file_ids: Vec<u64>) -> Result<MergeResult> {
        if file_ids.is_empty() {
            return Ok(MergeResult::default())
        }
        // todo: check file ids are continued
        let older_data = self.older_data.read().expect("lock read");
        let hashmap = &older_data.hashmap;
        let mut new_hashmap: HashMap<Key, Position> = HashMap::with_capacity(hashmap.capacity());
        let mut next_file_id = self.config.min_merge_file_id;

        let mut new_file_ids = vec![next_file_id];
        let mut to_remove_file_ids = vec![];
        let mut new_segment = Segment::new(next_file_id, &self.path);
        next_file_id += 1;

        for file_id in &file_ids {
            let segment = Segment::open(*file_id, &self.path);
            for kv_result in segment.iter() {
                let entry = kv_result?;
                match hashmap.get(&entry.key) {
                    None => continue,
                    Some(&pos) => {
                        if segment.file_id == pos.file_id && entry.offset == pos.offset {
                            if new_segment.size >= self.config.max_size_per_segment {
                                new_file_ids.push(next_file_id);
                                new_segment = Segment::new(next_file_id, &self.path);
                                next_file_id += 1;
                            }
                            let offset = new_segment.insert(entry.key.clone(), entry.value)?;
                            new_hashmap.insert(entry.key, Position { file_id: new_segment.file_id, offset });
                        }
                    }
                }
            }
            to_remove_file_ids.push(segment.file_id);
        }

        Ok(MergeResult {
            merged_hashmap: new_hashmap,
            new_file_ids,
            to_remove_file_ids,
        })
    }

    pub fn finish_merging(&self, mut merge_result: MergeResult) -> Result<()> {
        debug!(target: "bitcask::store::finish_merging", "new_file_ids: {:?}, to_remove_file_ids: {:?}", merge_result.new_file_ids, merge_result.to_remove_file_ids);
        assert!(merge_result.new_file_ids.len() <= merge_result.to_remove_file_ids.len());

        let mut older_data = self.older_data.write().expect("lock write");
        let mut hashmap = HashMap::new();
        mem::swap(&mut hashmap, &mut merge_result.merged_hashmap);

        for i in &merge_result.to_remove_file_ids {
            older_data.remove_segment(*i)?;
        }
        let mut mapping = HashMap::new();
        for (i, from_file_id) in merge_result.new_file_ids.iter().enumerate() {
            let to_file_id = merge_result.to_remove_file_ids[i];
            self.rename_segment(*from_file_id, to_file_id)?;
            mapping.insert(*from_file_id, to_file_id);
            older_data.add_segment(Segment::open(to_file_id, &self.path));
        }
        for v in hashmap.values_mut() {
            v.file_id = mapping[&v.file_id];
        }

        older_data.hashmap.extend(hashmap);
        Ok(())
    }
}

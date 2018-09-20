use core::{Config, Key, Result, Value};
use hint::Hint;
use keys_iterator::StoreKeys;
use regex::bytes::Regex;
use segment::{Offset, Segment};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, rename};
use std::hash::Hash;
use std::mem;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub const TOMBSTONE: &str = "<<>>";
pub const ESCAPED_TOMBSTONE: &str = "<<>><<>>";

lazy_static! {
    static ref TOMBSTONE_REGEXP: Regex = Regex::new(TOMBSTONE).expect("regexp");
    static ref ESCAPED_TOMBSTONE_REGEXP: Regex = Regex::new(ESCAPED_TOMBSTONE).expect("regexp");
}

pub fn escape_tombstone(value: Value) -> Value {
    TOMBSTONE_REGEXP
        .replace_all(&value, ESCAPED_TOMBSTONE.as_bytes())
        .into_owned()
}

pub fn unescape_tombstone(value: Value) -> Value {
    ESCAPED_TOMBSTONE_REGEXP
        .replace_all(&value, TOMBSTONE.as_bytes())
        .into_owned()
}

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
    active_hint: Hint,
    active_hashmap: HashMap<Key, Position>,
    pending_segments: HashMap<u64, Segment>,
    pending_hints: HashMap<u64, Hint>,
    pending_hashmap: HashMap<Key, Position>,
    config: Arc<Config>,
}

impl ActiveData {
    pub fn get<Q>(&self, key: &Q) -> Result<Option<Value>>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(pos) = self.active_hashmap.get(key) {
            return self.active_segment.get(pos.offset);
        }

        if let Some(pos) = self.pending_hashmap.get(key) {
            return self
                .pending_segments
                .get(&pos.file_id)
                .map_or(Ok(None), |s| s.get(pos.offset));
        }
        Ok(None)
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<bool> {
        let active_segment = &mut self.active_segment;
        let offset = active_segment.insert(key.clone(), value)?;
        let file_id = active_segment.file_id;
        let position = Position { offset, file_id };
        self.active_hint.insert(&key, position)?;
        let active_hashmap = &mut self.active_hashmap;
        active_hashmap.insert(key, position);

        Ok(active_segment.size >= self.config.max_size_per_segment)
    }

    pub fn rotate(&mut self, mut segment: Segment, mut hint: Hint) {
        let mut new_active_hashmap = HashMap::with_capacity(100);
        mem::swap(&mut new_active_hashmap, &mut self.active_hashmap);
        self.pending_hashmap.extend(new_active_hashmap);

        assert_eq!(segment.file_id, hint.file_id);

        mem::swap(&mut self.active_segment, &mut segment);
        self.pending_segments.insert(segment.file_id, segment);

        mem::swap(&mut self.active_hint, &mut hint);
        self.pending_hints.insert(hint.file_id, hint);
    }
    //
    //    pub fn delete(&mut self, key: Key) -> Result<bool> {
    //        self.insert_raw(key, TOMBSTONE.as_bytes().to_vec())
    //    }

    pub fn exists(&self, key: &Key) -> Result<bool> {
        Ok(match self.active_hashmap.get(key) {
            Some(v) => *v != Position::not_exist(),
            None => match self.pending_hashmap.get(key) {
                None => false,
                Some(v) => *v != Position::not_exist(),
            },
        })
    }

    pub fn keys<'a>(&'a self) -> Box<Iterator<Item = &'a Key> + 'a> {
        Box::new(
            self.active_hashmap
                .keys()
                .chain(self.pending_hashmap.keys()),
        )
    }
}

pub struct OlderData {
    segments: HashMap<u64, Segment>,
    hints: HashMap<u64, Hint>,
    hashmap: HashMap<Key, Position>,
    config: Arc<Config>,
}

impl OlderData {
    pub fn get<Q>(&self, key: &Q) -> Result<Option<Value>>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if let Some(pos) = self.hashmap.get(key) {
            return self
                .segments
                .get(&pos.file_id)
                .map_or(Ok(None), |s| s.get(pos.offset));
        }
        Ok(None)
    }

    pub fn add_segment(&mut self, segment: Segment, hint: Hint) {
        assert_eq!(segment.file_id, hint.file_id);
        self.segments.insert(segment.file_id, segment);
        self.hints.insert(hint.file_id, hint);
    }

    fn remove_segment(&mut self, file_id: u64) -> Result<()> {
        let seg = self.segments.remove(&file_id);
        if let Some(mut seg) = seg {
            seg.destroy()?;
        }
        let hint = self.hints.remove(&file_id);
        if let Some(mut h) = hint {
            h.destroy()?;
        }
        Ok(())
    }

    pub fn keys<'a>(&'a self) -> Box<Iterator<Item = &'a Key> + 'a> {
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
                hints: HashMap::new(),
                hashmap: HashMap::new(),
                config: config.clone(),
            }),
            active_data: RwLock::new(ActiveData {
                active_segment: Segment::new(0, path),
                active_hint: Hint::new(0, path),
                active_hashmap: HashMap::with_capacity(100),
                pending_segments: HashMap::with_capacity(10),
                pending_hints: HashMap::with_capacity(100),
                pending_hashmap: HashMap::with_capacity(100),
                config: config.clone(),
            }),
            config: config.clone(),
        }
    }

    pub fn open(config: Arc<Config>) -> Self {
        let path = &config.path;

        if !path.exists() {
            create_dir_all(path).expect("create dir");
        }

        let mut hashmap = HashMap::with_capacity(100);
        let mut segments = HashMap::with_capacity(100);
        let mut hints = HashMap::with_capacity(100);
        let mut max_file_id = 0;
        for entry in read_dir(path).expect("read segments dir") {
            let entry = entry.expect("read path entry");
            let segment_path = entry.path();
            if segment_path
                .extension()
                .expect("get extension")
                .to_string_lossy()
                != "data"
            {
                continue;
            }
            let file_id = segment_path
                .file_stem()
                .expect("get file id")
                .to_str()
                .expect("to string")
                .parse::<u64>()
                .expect("parse int");
            max_file_id = max_file_id.max(file_id);
            let seg = Segment::open(file_id, path);
            let mut hint = Hint::open(file_id, path);
            match hint {
                Ok(ref hint) => {
                    for entry_result in hint {
                        let entry = entry_result.expect("get hint entry");
                        hashmap.insert(entry.key.clone(), entry.position);
                    }
                }
                Err(_) => {
                    let mut h = Hint::new(file_id, path);
                    for entry_result in &seg {
                        let entry = entry_result.expect("get entry");
                        let pos = Position {
                            file_id,
                            offset: entry.offset,
                        };
                        h.insert(&entry.key, pos).expect("insert");
                        hashmap.insert(entry.key, pos);
                    }
                    hint = Ok(h);
                }
            }

            debug!(target: "bitcask::store::open", "add segment: {:?}", file_id);
            segments.insert(file_id, seg);
            hints.insert(file_id, hint.unwrap());
        }
        Store {
            path: path.clone(),
            next_file_id: RwLock::new(max_file_id + 2),
            older_data: RwLock::new(OlderData {
                segments,
                hints,
                hashmap,
                config: config.clone(),
            }),
            active_data: RwLock::new(ActiveData {
                active_segment: Segment::new(max_file_id + 1, path),
                active_hint: Hint::new(max_file_id + 1, path),
                active_hashmap: HashMap::with_capacity(100),
                pending_segments: HashMap::with_capacity(10),
                pending_hints: HashMap::with_capacity(10),
                pending_hashmap: HashMap::with_capacity(100),
                config: config.clone(),
            }),
            config: config.clone(),
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Result<Option<Value>>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let ret = self.active_data.read().expect("lock read").get(key)?;
        if let Some(v) = ret {
            if v.as_slice() == TOMBSTONE.as_bytes() {
                return Ok(None);
            }
            return Ok(Some(unescape_tombstone(v)));
        }

        let ret = self.older_data.read().expect("lock read").get(key)?;
        if let Some(v) = ret {
            if v.as_slice() == TOMBSTONE.as_bytes() {
                return Ok(None);
            }
            return Ok(Some(unescape_tombstone(v)));
        }

        Ok(None)
    }

    pub fn insert(&self, key: Key, value: Value) -> Result<()> {
        self.insert_raw(key, escape_tombstone(value))
    }

    fn insert_raw(&self, key: Key, value: Value) -> Result<()> {
        let mut active_data = self.active_data.write().expect("lock write");
        let to_rotate = active_data.insert(key, value)?;
        if to_rotate {
            let mut next_file_id = self.next_file_id.write().expect("lock write");
            let file_id = *next_file_id;
            *next_file_id += 1;
            active_data.rotate(
                Segment::new(file_id, &self.path),
                Hint::new(file_id, &self.path),
            );
            assert!(file_id < self.config.max_file_id);
        }

        if !active_data.pending_hashmap.is_empty() {
            assert!(!active_data.pending_hints.is_empty());

            if let Ok(mut older_data) = self.older_data.try_write() {
                let mut pending_segments = HashMap::new();
                let mut pending_hints = HashMap::new();
                let mut pending_hashmap = HashMap::new();
                mem::swap(&mut active_data.pending_segments, &mut pending_segments);
                mem::swap(&mut active_data.pending_hints, &mut pending_hints);
                mem::swap(&mut active_data.pending_hashmap, &mut pending_hashmap);

                older_data.segments.extend(pending_segments);
                older_data.hints.extend(pending_hints);
                older_data.hashmap.extend(pending_hashmap);
            }
        }

        Ok(())
    }

    pub fn delete(&self, key: Key) -> Result<()> {
        self.insert_raw(key, TOMBSTONE.as_bytes().to_vec())
    }
    pub fn exists<Q>(&self, key: &Q) -> Result<bool>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        Ok(self.get(key)?.is_some())
    }

    fn remove_segment(&self, file_id: u64) -> Result<()> {
        self.older_data
            .write()
            .expect("lock write")
            .remove_segment(file_id)
    }

    fn rename_segment(&self, from: u64, to: u64) -> Result<()> {
        rename(
            Segment::get_path(from, &self.path),
            Segment::get_path(to, &self.path),
        )?;
        rename(
            Hint::get_path(from, &self.path),
            Hint::get_path(to, &self.path),
        )?;
        Ok(())
    }

    pub fn keys(&self) -> StoreKeys {
        StoreKeys {
            active_data_guard: self.active_data.read().expect("lock read"),
            older_data_guard: self.older_data.read().expect("lock read"),
        }
    }

    pub fn prepare_full_merging(&self) -> Vec<u64> {
        self.older_data
            .read()
            .expect("lock read")
            .segments
            .keys()
            .cloned()
            .map(|s| s)
            .collect()
    }

    pub fn prepare_merging_since(&self, file_id: u64) -> Vec<u64> {
        self.older_data
            .read()
            .expect("lock read")
            .segments
            .keys()
            .cloned()
            .map(|s| s)
            .filter(|s| *s >= file_id)
            .collect()
    }

    pub fn merge(&self, file_ids: &[u64]) -> Result<MergeResult> {
        if file_ids.is_empty() {
            return Ok(MergeResult::default());
        }
        // todo: check file ids are continued
        let older_data = self.older_data.read().expect("lock read");
        let hashmap = &older_data.hashmap;
        let mut new_hashmap: HashMap<Key, Position> = HashMap::with_capacity(hashmap.capacity());
        let mut next_file_id = self.config.min_merge_file_id;

        let mut new_file_ids = vec![next_file_id];
        let mut to_remove_file_ids = vec![];
        let mut new_segment = Segment::new(next_file_id, &self.path);
        let mut new_hint = Hint::new(next_file_id, &self.path);
        next_file_id += 1;

        for file_id in file_ids {
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
                                new_hint = Hint::new(next_file_id, &self.path);
                                next_file_id += 1;
                            }
                            let offset = new_segment.insert(entry.key.clone(), entry.value)?;
                            let pos = Position {
                                file_id: new_segment.file_id,
                                offset,
                            };
                            new_hint.insert(&entry.key, pos)?;
                            new_hashmap.insert(entry.key, pos);
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
            older_data.add_segment(
                Segment::open(to_file_id, &self.path),
                Hint::open(to_file_id, &self.path)?,
            );
        }
        for v in hashmap.values_mut() {
            v.file_id = mapping[&v.file_id];
        }

        older_data.hashmap.extend(hashmap);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn bench_escape_tombstone(b: &mut Bencher) {
        let value: Vec<u8> = vec![0; 512];
        b.iter(|| escape_tombstone(value.clone()));
    }

    #[test]
    fn it_can_escape() {
        assert_eq!(
            escape_tombstone(b"<<>>".to_vec()),
            "<<>><<>>".as_bytes().to_vec()
        );
        assert_eq!(
            escape_tombstone(b"aa<<>>hel<<>>sdf".to_vec()),
            "aa<<>><<>>hel<<>><<>>sdf".as_bytes().to_vec()
        );
        assert_eq!(
            escape_tombstone(b"<<>><<>>".to_vec()),
            "<<>><<>><<>><<>>".as_bytes().to_vec()
        );
    }

    #[test]
    fn it_can_unescape() {
        assert_eq!(
            unescape_tombstone(b"<<>><<>>".to_vec()),
            "<<>>".as_bytes().to_vec()
        );
        assert_eq!(
            unescape_tombstone(b"aa<<>><<>>hel<<>><<>>sdf".to_vec()),
            "aa<<>>hel<<>>sdf".as_bytes().to_vec()
        );
        assert_eq!(
            unescape_tombstone(b"<<>><<>><<>><<>>".to_vec()),
            "<<>><<>>".as_bytes().to_vec()
        );
        assert_eq!(
            unescape_tombstone(b"<<>>".to_vec()),
            "<<>>".as_bytes().to_vec()
        );
    }

}

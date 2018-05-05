use ::core::{Key, Result, Value};
use ::segment::{Offset, Segment, Size};
use std::collections::HashMap;
use std::fs::read_dir;
use std::path::PathBuf;


pub struct Store {
    path: PathBuf,
    segments: Vec<Segment>,
    current_segment_index: usize,
    max_size_per_segment: Size,
    version: usize,
}


impl Store {
    pub fn new(path: &PathBuf, version: usize) -> Self {
        let mut store = Store {
            path: path.clone(),
            segments: vec![],
            current_segment_index: 0,
            max_size_per_segment: 100,
            version,
        };

        store.new_segment(0);
        store.current_segment_index = 0;
        store
    }

    pub fn open(path: &PathBuf) -> Self {
        let mut store = Store {
            path: path.clone(),
            segments: vec![],
            current_segment_index: 0,
            max_size_per_segment: 100,
            version: 0,
        };

        let version = Store::find_latest_data_version(path).expect("find latest version").unwrap();
        let segment_dir = path.join(format!("{}", version));
        for entry in read_dir(&segment_dir).expect("read dir") {
            let entry = entry.expect("read path entry");
            let segment_path = entry.path();
            let seg = Segment::open(segment_path);
            store.add_segment(seg);
        }
        store.current_segment_index = store.segments.len() - 1;
        store.version = version;
        store
    }

    fn find_latest_data_version(path: &PathBuf) -> Result<Option<usize>> {
        let current_version = read_dir(path)?.filter_map(|entry| {
            if let Ok(entry) = entry {
                if let Ok(ftype) = entry.file_type() {
                    if ftype.is_dir() {
                        let path = entry.path();
                        let dir_name = path.file_name();
                        let version = dir_name
                            .and_then(|s| s.to_str())
                            .map(|s| s.parse::<usize>().unwrap());
                        return version;
                    }
                }
            }
            None
        }).max();
        Ok(current_version)
    }

    fn get_segment_index_by_offset(&self, offset: Offset) -> Result<Option<usize>> {
        Ok(self.segments.iter().rposition(|seg| seg.file_begin_offset() <= offset))
    }

    fn add_segment(&mut self, segment: Segment) {
        self.segments.push(segment);
        self.segments.sort_by_key(|seg| seg.file_begin_offset());
        self.current_segment_index += 1;
    }

    fn new_segment(&mut self, offset: Offset) {
        let data_path = self.path.join(format!("{}", self.version));
        let seg = Segment::new(offset, data_path);
        self.add_segment(seg);
    }

    pub fn get(&mut self, offset: Offset) -> Result<Option<Value>> {
        let index = match self.get_segment_index_by_offset(offset)? {
            Some(index) => index,
            None => return Ok(None)
        };

        let segment = &mut self.segments[index];
        segment.get(offset - segment.file_begin_offset())
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<Offset> {
        let segment = &mut self.segments[self.current_segment_index];
        let inserted_offset = segment.insert(key, value)?;
        let file_end_offset = segment.file_end_offset();
        let file_begin_offset = segment.file_begin_offset();
        if segment.file_size() >= self.max_size_per_segment {
            self.new_segment(file_end_offset)
        }
        Ok(inserted_offset + file_begin_offset)
    }

    pub fn compact(&mut self, hashmap: &HashMap<Key, Offset>) -> Result<(Store, HashMap<Key, Offset>)> {
        let mut store = Store::new(&self.path, self.version + 1);
        let mut new_hashmap: HashMap<Key, Offset> = HashMap::with_capacity(hashmap.capacity());
        for segment in &mut self.segments {
            let file_end_offset = segment.file_end_offset();
            let file_begin_offset = segment.file_begin_offset();

            for kv_result in segment.iter() {
                let entry = kv_result?;
                match hashmap.get(&entry.key) {
                    None => continue,
                    Some(&offset) => {
                        if offset >= file_begin_offset && offset < file_end_offset {
                            let offset = store.insert(entry.key.clone(), entry.value)?;
                            new_hashmap.insert(entry.key, offset);
                        }
                    }
                }
            }
        }
        Ok((store, new_hashmap))
    }

    pub fn build_hashmap(&mut self) -> Result<HashMap<Key, Offset>> {
        let mut new_hashmap: HashMap<Key, Offset> = HashMap::with_capacity(100);
        for segment in &mut self.segments {
            for entry_result in segment.iter() {
                let entry = entry_result?;
                new_hashmap.insert(entry.key.clone(), entry.offset);
            }
        }

        Ok(new_hashmap)
    }
}

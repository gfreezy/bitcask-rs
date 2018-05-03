use ::core::{Key, Result, Value};
use ::segment::{Offset, Segment, Size};
use std::collections::HashMap;
use std::path::PathBuf;


pub struct Store {
    path: PathBuf,
    segments: Vec<Segment>,
    current_segment_index: usize,
    max_size_per_segment: Size,
    version: usize,
}


impl Store {
    pub fn new(path: PathBuf, version: usize) -> Self {
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

    fn get_segment_index_by_offset(&self, offset: Offset) -> Result<Option<usize>> {
        Ok(self.segments.iter().rposition(|seg| seg.file_begin_offset() <= offset))
    }

    fn new_segment(&mut self, offset: Offset) {
        let data_path = self.path.join(format!("{}", self.version));
        let seg = Segment::new(offset, data_path);
        self.segments.push(seg);
        self.current_segment_index += 1;
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
        let mut store = Store::new(self.path.clone(), self.version + 1);
        let mut new_hashmap: HashMap<Key, Offset> = HashMap::with_capacity(hashmap.capacity());
        for segment in &mut self.segments {
            let file_end_offset = segment.file_end_offset();
            let file_begin_offset = segment.file_begin_offset();

            for kv_result in segment.iter() {
                let (key, value) = kv_result?;
                match hashmap.get(&key) {
                    None => continue,
                    Some(&offset) => {
                        if offset >= file_begin_offset && offset < file_end_offset {
                            let offset = store.insert(key.clone(), value)?;
                            new_hashmap.insert(key, offset);
                        }
                    }
                }
            }
        }
        Ok((store, new_hashmap))
    }
}

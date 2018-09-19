use core::{Key, Result, Value};
use integer_encoding::{VarInt, VarIntReader, VarIntWriter};
use io_at::Cursor;
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, BufReader, BufWriter};
use std::path::PathBuf;
use std::hash::Hasher;
use twox_hash::XxHash;


fn xxhash32(bufs: &[&[u8]]) -> u32 {
    let mut hash = XxHash::with_seed(0);
    for buf in bufs {
        hash.write(buf)
    }
    hash.finish() as u32
}


pub type Offset = u64;

struct SegmentEntry {
    key: Key,
    value: Value,
}

impl SegmentEntry {
    pub fn new_checking_hash(key: Key, value: Value, hash: u32) -> SegmentEntry {
        let entry = Self::new(key, value);
        assert_eq!(hash, entry.compute_hash());
        entry
    }

    pub fn new(key: Key, value: Value) -> SegmentEntry {
        SegmentEntry {
            key: key,
            value: value,
        }
    }

    pub fn compute_size(&self) -> u64 {
        let key_buf = self.key.as_slice();
        let key_size_length = (key_buf.len() as u64).required_space();
        let value_buf = self.value.as_slice();
        let value_size_length = (value_buf.len() as u64).required_space();
        let hash_length = self.compute_hash().required_space();
        let size = key_size_length as u64
            + value_size_length as u64
            + key_buf.len() as u64
            + value_buf.len() as u64
            + hash_length as u64;
        size
    }

    pub fn compute_hash(&self) -> u32 {
        xxhash32(&[self.key.as_slice(), self.value.as_slice()])
    }
}

fn read_from_cursor(file: &mut BufReader<Cursor<&File>>) -> Result<SegmentEntry> {
    let key_size = file.read_varint::<u64>()?;
    debug!(target: "bitcask::segment", "get key size {}", key_size);
    let mut key_buf = vec![0; key_size as usize];
    file.read_exact(&mut key_buf)?;
    debug!(target: "bitcask::segment", "get key buf {:?}", key_buf);
    let value_size = file.read_varint::<u64>()?;
    debug!(target: "bitcask::segment", "get value size {}", value_size);
    let mut value_buf = vec![0; value_size as usize];
    file.read_exact(&mut value_buf)?;
    debug!(target: "bitcask::segment", "get value buf {:?}", value_buf);
    let hash = file.read_varint::<u32>()?;
    debug!(target: "bitcask::segment", "get hash {}", hash);
    Ok(SegmentEntry::new_checking_hash(
        key_buf,
        value_buf,
        hash
    ))
}


fn write_at_cursor(entry: &SegmentEntry, file: &mut BufWriter<Cursor<&File>>) -> Result<u64> {
    let key_buf = entry.key.as_slice();
    debug!(target: "bitcask::segment", "insert key size {}", key_buf.len());
    let _key_size_length = file.write_varint(key_buf.len() as u64)?;
    debug!(target: "bitcask::segment", "insert key buf {:?}", key_buf);
    file.write_all(key_buf)?;
    let value_buf = entry.value.as_slice();
    debug!(target: "bitcask::segment", "insert value size {}", value_buf.len());
    let _value_size_length = file.write_varint(value_buf.len() as u64)?;
    debug!(target: "bitcask::segment", "insert value buf {:?}", value_buf);
    file.write_all(value_buf)?;
    let hash = entry.compute_hash();
    debug!(target: "bitcask::segment", "insert hash {:?}", hash);
    let _hash_length = file.write_varint(hash)?;
    Ok(entry.compute_size())
}

pub struct Segment {
    file_path: PathBuf,
    pub file_id: u64,
    file: Option<File>,
    pub size: u64,
}

impl Segment {
    pub fn get_path(file_id: u64, path: &PathBuf) -> PathBuf {
        path.join(format!("{}.data", file_id))
    }

    pub fn new(file_id: u64, path: &PathBuf) -> Self {
        create_dir_all(&path).expect("create dir");
        let file_path = Self::get_path(file_id, path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&file_path)
            .expect("open segment file");

        debug!(target: "bitcask::segment", "new segment file {:?}", &file_path);
        Segment {
            file_id,
            file_path,
            file: Some(file),
            size: 0,
        }
    }

    pub fn open(file_id: u64, path: &PathBuf) -> Self {
        let file_path = Self::get_path(file_id, path);
        let mut file = OpenOptions::new()
            .read(true)
            .open(&file_path)
            .expect("open segment file");

        let size = file.seek(SeekFrom::End(0)).expect("find file size");
        Segment {
            file_id,
            file_path: file_path.clone(),
            file: Some(file),
            size,
        }
    }

    pub fn get(&self, offset: Offset) -> Result<Option<Value>> {
        let mut file = BufReader::new(Cursor::new(self.file.as_ref().expect("get file"), offset));
        Ok(Some(read_from_cursor(&mut file)?.value))
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<Offset> {
        let offset = self.size;
        let mut file = BufWriter::new(Cursor::new(self.file.as_ref().expect("get file"), offset));
        let entry = SegmentEntry::new(key, value);
        self.size += write_at_cursor(&entry, &mut file)?;
        Ok(offset)
    }

    pub fn destroy(&mut self) -> Result<()> {
        self.file = None;
        remove_file(&self.file_path)?;
        Ok(())
    }

    pub fn iter(&self) -> SegmentIterator {
        SegmentIterator::new(self)
    }
}

pub struct SegmentIterator<'a> {
    segment: &'a Segment,
    offset: u64,
}

impl<'a> IntoIterator for &'a Segment {
    type Item = Result<Entry>;
    type IntoIter = SegmentIterator<'a>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        SegmentIterator::new(self)
    }
}

impl<'a> SegmentIterator<'a> {
    fn new(segment: &'a Segment) -> SegmentIterator<'a> {
        SegmentIterator { segment, offset: 0 }
    }
}

pub struct Entry {
    pub offset: u64,
    pub key: Key,
    pub value: Value,
}

impl<'a> Iterator for SegmentIterator<'a> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.offset >= self.segment.size {
            return None;
        }

        debug!(target: "bitcask::segment",
               "file path: {:?}, offset: {}, size: {}",
            &self.segment.file_path,
            self.offset,
            self.segment.size
        );
        let mut file = BufReader::new(Cursor::new(self.segment.file.as_ref().expect("get file"), self.offset));
        let segment_entry = read_from_cursor(&mut file).expect("read from cursor");
        let size = segment_entry.compute_size();
        let entry = Entry {
            key: segment_entry.key,
            value: segment_entry.value,
            offset: self.offset,
        };

        self.offset += size;

        Some(Ok(entry))
    }
}

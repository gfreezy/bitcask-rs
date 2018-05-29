use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use core::{Key, Result, Value};
use std::fs::{create_dir_all, File, OpenOptions, remove_file};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use io_at::Cursor;


pub type Offset = u64;

const LENGTH_SIZE: u64 = 8;

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
        let mut file = Cursor::new(self.file.as_ref().expect("get file"), offset);
        let size = file.read_u64::<BigEndian>()?;
        debug!(target: "bitcask::segment", "get key size {}", size);
        let mut key_buf = vec![0; size as usize];
        file.read_exact(&mut key_buf)?;
        debug!(target: "bitcask::segment", "get key buf {:?}", key_buf);
        let size = file.read_u64::<BigEndian>()?;
        debug!(target: "bitcask::segment", "get value size {}", size);
        let mut value_buf = vec![0; size as usize];
        file.read_exact(&mut value_buf)?;
        debug!(target: "bitcask::segment", "get value buf {:?}", value_buf);
        Ok(Some(value_buf))
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<Offset> {
        let offset = self.size;
        let mut file = Cursor::new(self.file.as_mut().expect("get file"), offset);
        let key_buf = key.as_bytes();
        debug!(target: "bitcask::segment", "insert key size {}", key_buf.len());
        file.write_u64::<BigEndian>(key_buf.len() as u64)?;
        debug!(target: "bitcask::segment", "insert key buf {:?}", key_buf);
        file.write_all(key_buf)?;
        let value_buf = value.as_slice();
        debug!(target: "bitcask::segment", "insert value size {}", value_buf.len());
        file.write_u64::<BigEndian>(value_buf.len() as u64)?;
        debug!(target: "bitcask::segment", "insert value buf {:?}", value_buf);
        file.write_all(value_buf)?;
        self.size += LENGTH_SIZE * 2 + key_buf.len() as u64 + value_buf.len() as u64;
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

impl<'a> SegmentIterator<'a> {
    fn new(segment: &'a Segment) -> SegmentIterator<'a> {
        SegmentIterator {
            segment,
            offset: 0,
        }
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
        let mut file = Cursor::new(self.segment.file.as_ref().expect("get file"), self.offset);
        let key_size = file.read_u64::<BigEndian>().expect("read key size");
        debug!(target: "bitcask::segment", "read key size: {}", key_size);
        let mut key_buf = vec![0; key_size as usize];
        file.read_exact(&mut key_buf).expect("read key");
        let key = String::from_utf8(key_buf).unwrap();
        debug!(target: "bitcask::segment", "read key: {:?}", &key);
        let value_size = file.read_u64::<BigEndian>().expect("read value size");
        debug!(target: "bitcask::segment", "read value size: {:?}", value_size);
        let mut value_buf = vec![0; value_size as usize];
        file.read_exact(&mut value_buf).expect("read value");
        debug!(target: "bitcask::segment", "read value: {:?}", &value_buf);

        let entry = Entry {
            key,
            value: value_buf,
            offset: self.offset,
        };

        self.offset += LENGTH_SIZE * 2 + key_size + value_size;

        Some(Ok(entry))
    }
}

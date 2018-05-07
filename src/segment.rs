use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use core::{Key, Result, TOMBSTONE, Value};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use io_at::Cursor;


pub type Offset = u64;
pub type Size = u64;

const LENGTH_SIZE: u64 = 8;

pub struct Segment {
    begin_offset: Offset,
    file_path: PathBuf,
    file: File,
    size: u64,
}

impl Segment {
    pub fn new(begin_offset: Offset, path: PathBuf) -> Self {
        create_dir_all(&path).expect("create dir");
        let file_path = path.join(format!("{}.data", begin_offset));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&file_path)
            .expect("open segment file");

        Segment {
            begin_offset,
            file_path,
            file,
            size: 0,
        }
    }

    pub fn open(file_path: PathBuf) -> Self {
        let mut file = OpenOptions::new()
            .read(true)
            .open(&file_path)
            .expect("open segment file");

        let begin_offset: u64 = file_path.file_stem()
            .and_then(|offset| offset.to_str())
            .map(|s| s.parse().expect("parse file offset"))
            .unwrap();
        let size = file.seek(SeekFrom::End(0)).expect("find file size");
        Segment {
            begin_offset,
            file_path,
            file,
            size,
        }
    }

    pub fn get(&self, offset: Offset) -> Result<Option<Value>> {
        let mut file = Cursor::new(&self.file, offset);
        let size = file.read_u64::<BigEndian>()?;
        debug!("get key size {}", size);
        let mut key_buf = vec![0; size as usize];
        file.read_exact(&mut key_buf)?;
        debug!("get key buf {:?}", key_buf);
        let size = file.read_u64::<BigEndian>()?;
        debug!("get value size {}", size);
        let mut value_buf = vec![0; size as usize];
        file.read_exact(&mut value_buf)?;
        debug!("get value buf {:?}", value_buf);
        if value_buf == TOMBSTONE.as_bytes() {
            return Ok(None);
        }
        Ok(Some(value_buf))
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<Offset> {
        let offset = self.size;
        let mut file = Cursor::new(&mut self.file, offset);
        let key_buf = key.as_bytes();
        debug!("insert key size {}", key_buf.len());
        file.write_u64::<BigEndian>(key_buf.len() as u64)?;
        debug!("insert key buf {:?}", key_buf);
        file.write_all(key_buf)?;
        let value_buf = value.as_slice();
        debug!("insert value size {}", value_buf.len());
        file.write_u64::<BigEndian>(value_buf.len() as u64)?;
        debug!("insert value buf {:?}", value_buf);
        file.write_all(value_buf)?;
        self.size += LENGTH_SIZE * 2 + key_buf.len() as u64 + value_buf.len() as u64;
        Ok(offset)
    }

    pub fn file_size(&self) -> u64 {
        self.size
    }

    pub fn file_begin_offset(&self) -> Offset {
        self.begin_offset
    }

    pub fn file_end_offset(&self) -> Offset {
        self.begin_offset + self.file_size()
    }

    pub fn iter(&self) -> SegmentIterator {
        SegmentIterator::new(self)
    }
}

pub struct SegmentIterator<'a> {
    segment: &'a Segment,
    offset: u64,
    file_offset: u64,
}

impl<'a> SegmentIterator<'a> {
    fn new(segment: &'a Segment) -> SegmentIterator<'a> {
        let offset = segment.file_begin_offset();
        SegmentIterator {
            segment,
            file_offset: 0,
            offset,
        }
    }
}

pub struct Entry {
    pub offset: u64,
    pub key: Key,
    pub value: Value,
    pub file_offset: u64,
}


impl<'a> Iterator for SegmentIterator<'a> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.offset >= self.segment.file_end_offset() {
            return None;
        }

        debug!(
            "file path: {:?}, offset: {}, end offset: {}",
            &self.segment.file_path,
            self.offset,
            self.segment.file_end_offset()
        );
        let mut file = Cursor::new(&self.segment.file, self.file_offset);
        let key_size = file.read_u64::<BigEndian>().expect("read key size");
        debug!("read key size: {}", key_size);
        let mut key_buf = vec![0; key_size as usize];
        file.read_exact(&mut key_buf).expect("read key");
        let key = String::from_utf8(key_buf).unwrap();
        debug!("read key: {:?}", &key);
        let value_size = file.read_u64::<BigEndian>().expect("read value size");
        debug!("read value size: {:?}", value_size);
        let mut value_buf = vec![0; value_size as usize];
        file.read_exact(&mut value_buf).expect("read value");
        debug!("read value: {:?}", &value_buf);

        let entry = Entry {
            key,
            value: value_buf,
            offset: self.offset,
            file_offset: self.file_offset,
        };

        self.offset += LENGTH_SIZE * 2 + key_size + value_size;
        self.file_offset += LENGTH_SIZE * 2 + key_size + value_size;

        return Some(Ok(entry));
    }
}

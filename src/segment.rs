use ::core::{Key, Result, TOMBSTONE, Value};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;


pub type Offset = u64;
pub type Size = u64;

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
        let file = OpenOptions::new().create(true).write(true).truncate(true).read(true).open(
            &file_path).expect("open segment file");

        Segment {
            begin_offset,
            file_path,
            file,
            size: 0,
        }
    }

    pub fn get(&mut self, offset: Offset) -> Result<Option<Value>> {
        let read_offset = self.file.seek(SeekFrom::Start(offset))?;
        debug!("get from offset {}", read_offset);
        let size = self.file.read_u64::<BigEndian>()?;
        debug!("get key size {}", size);
        debug!("current offset: {}", self.file.seek(SeekFrom::Current(0))?);
        let mut key_buf = vec![0; size as usize];
        self.file.read_exact(&mut key_buf)?;
        debug!("current offset: {}", self.file.seek(SeekFrom::Current(0))?);
        debug!("get key buf {:?}", key_buf);
        let size = self.file.read_u64::<BigEndian>()?;
        debug!("get value size {}", size);
        let mut value_buf = vec![0; size as usize];
        self.file.read_exact(&mut value_buf)?;
        debug!("get value buf {:?}", value_buf);
        if value_buf == TOMBSTONE.as_bytes() {
            return Ok(None);
        }
        Ok(Some(value_buf))
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<Offset> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_buf = key.as_bytes();
        debug!("insert key size {}", key_buf.len());
        self.file.write_u64::<BigEndian>(key_buf.len() as u64)?;
        debug!("insert key buf {:?}", key_buf);
        self.file.write_all(key_buf)?;
        let value_buf = value.as_slice();
        debug!("insert value size {}", value_buf.len());
        self.file.write_u64::<BigEndian>(value_buf.len() as u64)?;
        debug!("insert value buf {:?}", value_buf);
        self.file.write_all(value_buf)?;
        self.size += (8 * 2 + key_buf.len() + value_buf.len()) as u64;
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

    pub fn iter(&mut self) -> SegmentIterator {
        SegmentIterator::new(self)
    }
}


pub struct SegmentIterator<'a> {
    segment: &'a mut Segment
}

impl<'a> SegmentIterator<'a> {
    fn new(segment: &'a mut Segment) -> SegmentIterator<'a> {
        let _ = segment.file.seek(SeekFrom::Start(0)).expect("unable to seek to begin");
        SegmentIterator {
            segment
        }
    }
}


impl<'a> Iterator for SegmentIterator<'a> {
    type Item = Result<(Key, Value)>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let offset = self.segment.file.seek(SeekFrom::Current(0)).expect("unable to seek to begin");
        if offset + self.segment.file_begin_offset() >= self.segment.file_end_offset() {
            return None;
        }

        debug!("file path: {:?}, offset: {}, end offset: {}", &self.segment.file_path, offset, self.segment.file_end_offset());
        let file = &mut self.segment.file;
        let size = file.read_u64::<BigEndian>().expect("read key size");
        debug!("read key size: {}", size);
        let mut key_buf = vec![0; size as usize];
        file.read_exact(&mut key_buf).expect("read key");
        let key = String::from_utf8(key_buf).unwrap();
        debug!("read key: {:?}", &key);
        let size = file.read_u64::<BigEndian>().expect("read value size");
        debug!("read value size: {:?}", size);
        let mut value_buf = vec![0; size as usize];
        file.read_exact(&mut value_buf).expect("read value");
        debug!("read value: {:?}", &value_buf);

        return Some(Ok((key, value_buf)));
    }
}


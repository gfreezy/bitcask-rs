use core::{Key, Result};
use integer_encoding::{VarInt, VarIntReader, VarIntWriter};
use io_at::Cursor;
use segment::Offset;
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use store::Position;

pub struct HintEntry {
    pub key: Key,
    pub key_size: u64,
    pub position: Position,
}

fn read_from_cursor(file: &mut Cursor<&File>) -> Result<HintEntry> {
    let key_size = file.read_varint::<u64>()?;
    debug!(target: "bitcask::hint::read_from_cursor", "get key size {}", key_size);
    let mut key_buf = vec![0; key_size as usize];
    file.read_exact(&mut key_buf)?;
    debug!(target: "bitcask::hint::read_from_cursor", "get key buf {:?}", key_buf);
    let file_id = file.read_varint::<u64>()?;
    debug!(target: "bitcask::hint::read_from_cursor", "get file id {}", file_id);
    let offset = file.read_varint::<u64>()?;
    debug!(target: "bitcask::hint::read_from_cursor", "get file pos {}", offset);
    Ok(HintEntry {
        key: key_buf,
        key_size,
        position: Position { file_id, offset },
    })
}

pub struct Hint {
    file_path: PathBuf,
    pub file_id: u64,
    file: Option<File>,
    pub size: u64,
}

impl Hint {
    pub fn get_path(file_id: u64, path: &PathBuf) -> PathBuf {
        path.join(format!("{}.hint", file_id))
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

        debug!(target: "bitcask::hint::new", "new hint file {:?}", &file_path);
        Hint {
            file_id,
            file_path,
            file: Some(file),
            size: 0,
        }
    }

    pub fn open(file_id: u64, path: &PathBuf) -> Result<Self> {
        let file_path = Self::get_path(file_id, path);
        let mut file = OpenOptions::new().read(true).open(&file_path)?;

        let size = file.seek(SeekFrom::End(0))?;
        Ok(Hint {
            file_id,
            file_path: file_path.clone(),
            file: Some(file),
            size,
        })
    }

    pub fn get(&self, offset: Offset) -> Result<Option<Position>> {
        let mut file = Cursor::new(self.file.as_ref().expect("get file"), offset);
        Ok(Some(read_from_cursor(&mut file)?.position))
    }

    pub fn insert(&mut self, key: &Key, position: Position) -> Result<Offset> {
        let offset = self.size;
        let mut file = Cursor::new(self.file.as_mut().expect("get file"), offset);
        let key_buf = key.as_slice();
        debug!(target: "bitcask::hint::insert", "insert key size {}", key_buf.len());
        let key_size_length = file.write_varint(key_buf.len() as u64)?;
        debug!(target: "bitcask::hint::insert", "insert key buf {:?}", key_buf);
        file.write_all(key_buf)?;
        debug!(target: "bitcask::hint::insert", "insert file id {:?}", position.file_id);
        let file_id_length = file.write_varint(position.file_id)?;
        debug!(target: "bitcask::hint::insert", "insert file offset {:?}", position.offset);
        let file_offset_length = file.write_varint(position.offset)?;

        self.size += key_size_length as u64
            + file_id_length as u64
            + file_offset_length as u64
            + key_buf.len() as u64;
        Ok(offset)
    }

    pub fn destroy(&mut self) -> Result<()> {
        self.file = None;
        remove_file(&self.file_path)?;
        Ok(())
    }

    pub fn iter(&self) -> HintIterator {
        self.into_iter()
    }
}

pub struct HintIterator<'a> {
    hint: &'a Hint,
    offset: u64,
}

impl<'a> IntoIterator for &'a Hint {
    type Item = Result<HintEntry>;
    type IntoIter = HintIterator<'a>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        HintIterator {
            hint: self,
            offset: 0,
        }
    }
}

impl<'a> Iterator for HintIterator<'a> {
    type Item = Result<HintEntry>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.offset >= self.hint.size {
            return None;
        }

        debug!(target: "bitcask::hint::HintIterator::next",
               "file path: {:?}, offset: {}, size: {}",
               &self.hint.file_path,
               self.offset,
               self.hint.size
        );
        let mut file = Cursor::new(self.hint.file.as_ref().expect("get file"), self.offset);
        let hint_entry = read_from_cursor(&mut file).expect("read from cursor");

        self.offset += hint_entry.key_size.required_space() as u64
            + hint_entry.position.file_id.required_space() as u64
            + hint_entry.position.offset.required_space() as u64
            + hint_entry.key_size;

        Some(Ok(hint_entry))
    }
}

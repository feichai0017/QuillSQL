use std::collections::BTreeMap;
use crate::error::Result;
use std::io::{Seek, SeekFrom, Write, Read};
use std::io::BufWriter;
use super::engine::{Engine, EngineIterator};

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;
const LOG_HEADER_SIZE: u32 = 8;


pub struct BitcaskEngine {
    keydir: KeyDir,
    log: Log,
}

impl Engine for BitcaskEngine {
    type EngineIterator<'a> = BitcaskEngineIterator;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> crate::error::Result<()> {
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;

        let val_size = value.len() as u32;
        self.keydir.insert(key, (offset + val_size as u64 - val_size as u64, val_size));

        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> crate::error::Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, val_size)) => {
                let val = self.log.read_entry(*offset, *val_size)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> crate::error::Result<()> {
        self.log.write_entry(&key, None)?;
        self.keydir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        todo!()
    }
}

pub struct BitcaskEngineIterator {
}

impl EngineIterator for BitcaskEngineIterator {}

impl Iterator for BitcaskEngineIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for BitcaskEngineIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct Log {
    file: std::fs::File,

}

impl Log {
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let val_size = value.map_or(0, |v| v.len() as u32);
        let entry_size = key_size + val_size + LOG_HEADER_SIZE;
        
        let mut writer = BufWriter::with_capacity(entry_size as usize, &self.file);
        writer.write_all(&key_size.to_be_bytes())?;
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;
        writer.write_all(&key)?;
        if let Some(v) = value {
            writer.write_all(v)?;
        }
        writer.flush()?;
        
        Ok((offset, entry_size))
    }

    fn read_entry(&mut self, offset: u64, val_size: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; val_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }
}
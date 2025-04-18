use crate::error::Result;
use std::sync::{Arc, Mutex};
use crate::storage::engine::Engine;

pub struct Mvcc<E: Engine>{
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Mvcc{engine: self.engine.clone()}
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(engine: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        Ok(MvccTransaction::begin(self.engine.clone()))
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> MvccTransaction<E> {
    pub fn begin(engine: Arc<Mutex<E>>) -> Self {
        Self {
            engine: engine
        }
    }
    
    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut engine = self.engine.lock()?;
        engine.set(key, value)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut engine = self.engine.lock()?;
        engine.get(key)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut engine = self.engine.lock()?;
        let mut iter = engine.scan_prefix(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult { key, value });
        }
        Ok(results)
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        let mut engine = self.engine.lock()?;
        engine.delete(key)
    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
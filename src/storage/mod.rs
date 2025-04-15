use crate::error::Result;

pub struct Mvcc{}

impl Clone for Mvcc {
    fn clone(&self) -> Self {
        Mvcc{}
    }
}

impl Mvcc {
    pub fn new() -> Self {
        Self {}
    }

    pub fn begin(&self) -> Result<MvccTransaction> {
        Ok(MvccTransaction::new())
    }
}

pub struct MvccTransaction {

}

impl MvccTransaction {
    pub fn new() -> Self {
        Self {}
    }
    
    
}
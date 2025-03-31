use std::ops::{Bound, RangeBounds};

use crate::error::Result;

// 抽象存储引擎接口定义，接入不同的存储引擎，目前支持内存和简单的磁盘 KV 存储
pub trait Engine {
    type EngineIterator<'a>: EngineIterator
    where
        Self: 'a;

    // 设置 key/value
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    // 获取 key 对应的数据
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    // 删除 key 对应的数据，如果 key 不存在的话则忽略
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    // 扫描
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_>;

    // 前缀扫描
    fn scan_prefix(&mut self, prefix: Vec<u8>) -> Self::EngineIterator<'_> {
        // start: aaaa
        // end:   aaab
        let start = Bound::Included(prefix.clone());
        let mut bound_prefix = prefix.clone();
        // 前缀包含了 255，例如 10,2,3,255
        // 右边界就是 10,2,4
        // 如果全都是 255
        // 右边界 Unbounded
        let end = match bound_prefix.iter().rposition(|b| *b != 255) {
            Some(pos) => {
                bound_prefix[pos] += 1;
                bound_prefix.truncate(pos + 1);
                Bound::Excluded(bound_prefix)
            }
            None => Bound::Unbounded,
        };

        self.scan((start, end))
    }
}

pub trait EngineIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

#[cfg(test)]
mod tests {
    use super::Engine;
    use crate::{
        error::Result,
        storage::{disk::DiskEngine, memory::MemoryEngine},
    };
    use std::{ops::Bound, path::PathBuf};

    // 测试点读的情况
    fn test_point_opt(mut eng: impl Engine) -> Result<()> {
        // 测试获取一个不存在的 key
        assert_eq!(eng.get(b"not exist".to_vec())?, None);

        // 获取一个存在的 key
        eng.set(b"aa".to_vec(), vec![1, 2, 3, 4])?;
        assert_eq!(eng.get(b"aa".to_vec())?, Some(vec![1, 2, 3, 4]));

        // 重复 put，将会覆盖前一个值
        eng.set(b"aa".to_vec(), vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"aa".to_vec())?, Some(vec![5, 6, 7, 8]));

        // 删除之后再读取
        eng.delete(b"aa".to_vec())?;
        assert_eq!(eng.get(b"aa".to_vec())?, None);

        // key、value 为空的情况
        assert_eq!(eng.get(b"".to_vec())?, None);
        eng.set(b"".to_vec(), vec![])?;
        assert_eq!(eng.get(b"".to_vec())?, Some(vec![]));

        eng.set(b"cc".to_vec(), vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"cc".to_vec())?, Some(vec![5, 6, 7, 8]));
        Ok(())
    }

    // 测试扫描
    fn test_scan(mut eng: impl Engine) -> Result<()> {
        eng.set(b"nnaes".to_vec(), b"value1".to_vec())?;
        eng.set(b"amhue".to_vec(), b"value2".to_vec())?;
        eng.set(b"meeae".to_vec(), b"value3".to_vec())?;
        eng.set(b"uujeh".to_vec(), b"value4".to_vec())?;
        eng.set(b"anehe".to_vec(), b"value5".to_vec())?;

        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"e".to_vec());

        let mut iter = eng.scan((start.clone(), end.clone()));
        let (key1, _) = iter.next().expect("no value founded")?;
        assert_eq!(key1, b"amhue".to_vec());

        let (key2, _) = iter.next().expect("no value founded")?;
        assert_eq!(key2, b"anehe".to_vec());
        drop(iter);

        let start = Bound::Included(b"b".to_vec());
        let end = Bound::Excluded(b"z".to_vec());
        let mut iter2 = eng.scan((start, end));

        let (key3, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key3, b"uujeh".to_vec());

        let (key4, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key4, b"nnaes".to_vec());

        let (key5, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key5, b"meeae".to_vec());

        Ok(())
    }

    // 测试前缀扫描
    fn test_scan_prefix(mut eng: impl Engine) -> Result<()> {
        eng.set(b"ccnaes".to_vec(), b"value1".to_vec())?;
        eng.set(b"camhue".to_vec(), b"value2".to_vec())?;
        eng.set(b"deeae".to_vec(), b"value3".to_vec())?;
        eng.set(b"eeujeh".to_vec(), b"value4".to_vec())?;
        eng.set(b"canehe".to_vec(), b"value5".to_vec())?;
        eng.set(b"aanehe".to_vec(), b"value6".to_vec())?;

        let prefix = b"ca".to_vec();
        let mut iter = eng.scan_prefix(prefix);
        let (key1, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key1, b"camhue".to_vec());
        let (key2, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key2, b"canehe".to_vec());

        Ok(())
    }

    #[test]
    fn test_memory() -> Result<()> {
        test_point_opt(MemoryEngine::new())?;
        test_scan(MemoryEngine::new())?;
        test_scan_prefix(MemoryEngine::new())?;
        Ok(())
    }

    #[test]
    fn test_disk() -> Result<()> {
        test_point_opt(DiskEngine::new(PathBuf::from("/tmp/sqldb1/db.log"))?)?;
        std::fs::remove_dir_all(PathBuf::from("/tmp/sqldb1"))?;

        test_scan(DiskEngine::new(PathBuf::from("/tmp/sqldb2/db.log"))?)?;
        std::fs::remove_dir_all(PathBuf::from("/tmp/sqldb2"))?;

        test_scan_prefix(DiskEngine::new(PathBuf::from("/tmp/sqldb3/db.log"))?)?;
        std::fs::remove_dir_all(PathBuf::from("/tmp/sqldb3"))?;
        Ok(())
    }
}

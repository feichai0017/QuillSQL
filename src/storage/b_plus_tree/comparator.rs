//! 提供B+树使用的各种键比较器
//! 用于在B+树查找、插入和删除操作中比较键的顺序

use std::cmp::Ordering;
use std::sync::Arc;

/// 比较器类型，用于比较两个二进制键
pub type KeyComparator = fn(&[u8], &[u8]) -> Ordering;

/// 比较器函数签名的Box类型，允许存储任意比较函数
pub type BoxedComparator = Box<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>;

/// 用于存储和共享比较器的引用计数类型
pub type ComparatorRef = Arc<BoxedComparator>;

/// 创建一个默认比较器的引用计数版本
pub fn create_default_comparator() -> ComparatorRef {
    Arc::new(Box::new(default_comparator))
}

/// 默认比较器 - 按字典序比较字节数组
pub fn default_comparator(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// 反向比较器 - 按字典序相反方向比较
pub fn reverse_comparator(a: &[u8], b: &[u8]) -> Ordering {
    b.cmp(a)
}

/// 长度优先比较器 - 先比较长度，再比较内容
pub fn length_first_comparator(a: &[u8], b: &[u8]) -> Ordering {
    match a.len().cmp(&b.len()) {
        Ordering::Equal => a.cmp(b),
        other => other,
    }
}

/// 前缀优先比较器 - 按照特定长度的前缀进行比较
pub fn prefix_comparator(prefix_len: usize) -> ComparatorRef {
    Arc::new(Box::new(move |a: &[u8], b: &[u8]| {
        let a_prefix = if a.len() >= prefix_len {
            &a[0..prefix_len]
        } else {
            &a
        };

        let b_prefix = if b.len() >= prefix_len {
            &b[0..prefix_len]
        } else {
            &b
        };

        a_prefix.cmp(b_prefix)
    }))
}

/// 组合比较器 - 使用多个比较器按顺序比较
pub fn chain_comparators(comparators: Vec<ComparatorRef>) -> ComparatorRef {
    Arc::new(Box::new(move |a: &[u8], b: &[u8]| {
        for comparator in &comparators {
            let result = comparator(a, b);
            if result != Ordering::Equal {
                return result;
            }
        }
        Ordering::Equal
    }))
}

/// 创建一个忽略特定字节偏移的比较器
pub fn offset_comparator(offset: usize) -> ComparatorRef {
    Arc::new(Box::new(move |a: &[u8], b: &[u8]| {
        if a.len() <= offset || b.len() <= offset {
            a.len().cmp(&b.len())
        } else {
            a[offset..].cmp(&b[offset..])
        }
    }))
}

/// 提供一个函数来从字符串名称获取比较器
pub fn get_comparator_by_name(name: &str) -> Option<ComparatorRef> {
    match name {
        "default" => Some(create_default_comparator()),
        "reverse" => Some(Arc::new(Box::new(reverse_comparator))),
        "length_first" => Some(Arc::new(Box::new(length_first_comparator))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_comparator() {
        assert_eq!(default_comparator(b"abc", b"def"), Ordering::Less);
        assert_eq!(default_comparator(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(default_comparator(b"def", b"abc"), Ordering::Greater);
    }

    #[test]
    fn test_reverse_comparator() {
        assert_eq!(reverse_comparator(b"abc", b"def"), Ordering::Greater);
        assert_eq!(reverse_comparator(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(reverse_comparator(b"def", b"abc"), Ordering::Less);
    }

    #[test]
    fn test_length_first_comparator() {
        assert_eq!(length_first_comparator(b"ab", b"abc"), Ordering::Less);
        assert_eq!(length_first_comparator(b"abc", b"def"), Ordering::Equal);
        assert_eq!(length_first_comparator(b"abcd", b"def"), Ordering::Greater);
    }

    #[test]
    fn test_prefix_comparator() {
        let comp = prefix_comparator(2);
        assert_eq!(comp(b"abcd", b"abef"), Ordering::Equal);
        assert_eq!(comp(b"abcd", b"acef"), Ordering::Less);
        assert_eq!(comp(b"ac", b"ab"), Ordering::Greater);
    }

    #[test]
    fn test_offset_comparator() {
        let comp = offset_comparator(2);
        assert_eq!(comp(b"xxabc", b"yyabc"), Ordering::Equal);
        assert_eq!(comp(b"xxabc", b"yydef"), Ordering::Less);
        assert_eq!(comp(b"xx", b"yyabc"), Ordering::Less); // 长度不足
    }
}

use crate::storage::b_plus_tree::codec::DecodedData;
use crate::error::{Error, Result};
use core::f32;
use std::f64;

pub struct CommonCodec;

impl CommonCodec {
    pub fn encode_bool(data: bool) -> Vec<u8> {
        if data {
            vec![1]
        } else {
            vec![0]
        }
    }

    pub fn decode_bool(bytes: &[u8]) -> Result<DecodedData<bool>> {
        if bytes.is_empty() {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                1
            )));
        }
        Ok((bytes[0] != 0, 1))
    }

    pub fn encode_bytes(data: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        // 先添加长度（4字节）
        bytes.extend(CommonCodec::encode_u32(data.len() as u32));
        // 再添加实际内容
        bytes.extend_from_slice(data);
        bytes
    }

    pub fn decode_bytes(bytes: &[u8]) -> Result<DecodedData<Vec<u8>>> {
        // 先解码长度
        let (length, offset) = CommonCodec::decode_u32(bytes)?;
        let length = length as usize;
        
        // 检查剩余字节是否足够
        if bytes.len() < offset + length {
            return Err(Error::Internal(format!(
                "bytes length {} is less than expected {}",
                bytes.len(),
                offset + length
            )));
        }
        
        // 提取数据
        let data = bytes[offset..offset + length].to_vec();
        Ok((data, offset + length))
    }

    pub fn encode_u8(data: u8) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_u8(bytes: &[u8]) -> Result<DecodedData<u8>> {
        if bytes.is_empty() {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                1
            )));
        }
        Ok((u8::from_be_bytes([bytes[0]]), 1))
    }

    pub fn encode_u16(data: u16) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_u16(bytes: &[u8]) -> Result<DecodedData<u16>> {
        if bytes.len() < 2 {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                2
            )));
        }
        let data = [bytes[0], bytes[1]];
        Ok((u16::from_be_bytes(data), 2))
    }

    pub fn encode_u32(data: u32) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_u32(bytes: &[u8]) -> Result<DecodedData<u32>> {
        if bytes.len() < 4 {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                4
            )));
        }
        let data = [bytes[0], bytes[1], bytes[2], bytes[3]];
        Ok((u32::from_be_bytes(data), 4))
    }

    pub fn encode_u64(data: u64) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_u64(bytes: &[u8]) -> Result<DecodedData<u64>> {
        if bytes.len() < 8 {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                8
            )));
        }
        let data = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        Ok((u64::from_be_bytes(data), 8))
    }

    pub fn encode_i8(data: i8) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_i8(bytes: &[u8]) -> Result<DecodedData<i8>> {
        if bytes.is_empty() {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                1
            )));
        }
        Ok((i8::from_be_bytes([bytes[0]]), 1))
    }

    pub fn encode_i16(data: i16) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_i16(bytes: &[u8]) -> Result<DecodedData<i16>> {
        if bytes.len() < 2 {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                2
            )));
        }
        let data = [bytes[0], bytes[1]];
        Ok((i16::from_be_bytes(data), 2))
    }

    pub fn encode_i32(data: i32) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_i32(bytes: &[u8]) -> Result<DecodedData<i32>> {
        if bytes.len() < 4 {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                4
            )));
        }
        let data = [bytes[0], bytes[1], bytes[2], bytes[3]];
        Ok((i32::from_be_bytes(data), 4))
    }

    pub fn encode_i64(data: i64) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_i64(bytes: &[u8]) -> Result<DecodedData<i64>> {
        if bytes.len() < 8 {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                8
            )));
        }
        let data = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        Ok((i64::from_be_bytes(data), 8))
    }

    pub fn encode_f32(data: f32) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_f32(bytes: &[u8]) -> Result<DecodedData<f32>> {
        if bytes.len() < 4 {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                4
            )));
        }
        let data = [bytes[0], bytes[1], bytes[2], bytes[3]];
        Ok((f32::from_be_bytes(data), 4))
    }

    pub fn encode_f64(data: f64) -> Vec<u8> {
        data.to_be_bytes().to_vec()
    }

    pub fn decode_f64(bytes: &[u8]) -> Result<DecodedData<f64>> {
        if bytes.len() < 8 {
            return Err(Error::Internal(format!(
                "bytes length {} is less than {}",
                bytes.len(),
                8
            )));
        }
        let data = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        Ok((f64::from_be_bytes(data), 8))
    }

    pub fn encode_string(data: &String) -> Vec<u8> {
        data.as_bytes().to_vec()
    }

    pub fn decode_string(bytes: &[u8]) -> Result<DecodedData<String>> {
        let data = String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::Internal(format!("Failed to decode string {}", e)))?;
        Ok((data, bytes.len()))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::b_plus_tree::codec::CommonCodec;

    #[test]
    fn common_codec() {
        assert!(
            CommonCodec::decode_bool(&CommonCodec::encode_bool(true))
                .unwrap()
                .0
        );
        assert!(
            !CommonCodec::decode_bool(&CommonCodec::encode_bool(false))
                .unwrap()
                .0
        );
        assert_eq!(
            5u8,
            CommonCodec::decode_u8(&CommonCodec::encode_u8(5u8))
                .unwrap()
                .0
        );
        assert_eq!(
            5u16,
            CommonCodec::decode_u16(&CommonCodec::encode_u16(5u16))
                .unwrap()
                .0
        );
        assert_eq!(
            5u32,
            CommonCodec::decode_u32(&CommonCodec::encode_u32(5u32))
                .unwrap()
                .0
        );
        assert_eq!(
            5u64,
            CommonCodec::decode_u64(&CommonCodec::encode_u64(5u64))
                .unwrap()
                .0
        );

        assert_eq!(
            5i8,
            CommonCodec::decode_i8(&CommonCodec::encode_i8(5i8))
                .unwrap()
                .0
        );
        assert_eq!(
            5i16,
            CommonCodec::decode_i16(&CommonCodec::encode_i16(5i16))
                .unwrap()
                .0
        );
        assert_eq!(
            5i32,
            CommonCodec::decode_i32(&CommonCodec::encode_i32(5i32))
                .unwrap()
                .0
        );
        assert_eq!(
            5i64,
            CommonCodec::decode_i64(&CommonCodec::encode_i64(5i64))
                .unwrap()
                .0
        );
        assert_eq!(
            5.0f32,
            CommonCodec::decode_f32(&CommonCodec::encode_f32(5.0f32))
                .unwrap()
                .0
        );
        assert_eq!(
            5.0f64,
            CommonCodec::decode_f64(&CommonCodec::encode_f64(5.0f64))
                .unwrap()
                .0
        );
        assert_eq!(
            "abc".to_string(),
            CommonCodec::decode_string(&CommonCodec::encode_string(&"abc".to_string()))
                .unwrap()
                .0
        );
    }
}
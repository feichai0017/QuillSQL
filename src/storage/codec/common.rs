use crate::error::{Error, Result};
use crate::storage::codec::DecodedData;
use core::f32;
use serde::{
    de::{self, IntoDeserializer},
    ser,
};
use std::f64;

pub fn serialize_key<T: serde::Serialize>(key: &T) -> Result<Vec<u8>> {
    let mut ser = Serializer { output: Vec::new() };
    key.serialize(&mut ser)?;
    Ok(ser.output)
}

pub fn deserialize_key<'a, T: serde::Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
    let mut der = Deserializer { input };
    T::deserialize(&mut der)
}

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

// Serde序列化器实现
pub struct Serializer {
    output: Vec<u8>,
}

impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleVariant = Self;
    type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.output.push(v as u8);
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.output.push(v);
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        let mut buf = [0; 4];
        let s = v.encode_utf8(&mut buf);
        self.serialize_str(s)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    // 原始值           编码后
    // 97 98 99     -> 97 98 99 0 0
    // 97 98 0 99   -> 97 98 0 255 99 0 0
    // 97 98 0 0 99 -> 97 98 0 255 0 255 99 0 0
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        let mut res = Vec::new();
        for e in v.into_iter() {
            match e {
                0 => res.extend([0, 255]),
                b => res.push(*b),
            }
        }
        // 放 0 0 表示结尾
        res.extend([0, 0]);

        self.output.extend(res);
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        // 使用0表示None
        self.output.push(0);
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        // 使用1表示Some，然后序列化内部值
        self.output.push(1);
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        // 空单元类型不需要额外存储
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        // 类似于unit，只是有名称
        self.serialize_unit()
    }

    // 类似 MvccKey::NextVersion
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        self.output.push(variant_index as u8);
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        // 直接序列化内部值
        value.serialize(self)
    }

    // 类似 TxnAcvtive(Version)
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        todo!()
    }

    // 类似 TxnWrite(Version, Vec<u8>)
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        // 由于SerializeMap是Impossible，我们返回一个错误
        Err(Error::Internal("Map serialization not supported".into()))
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        // 由于SerializeStruct是Impossible，我们返回一个错误
        Err(Error::Internal("Struct serialization not supported".into()))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        // 由于SerializeStructVariant是Impossible，我们返回一个错误
        Err(Error::Internal(
            "Struct variant serialization not supported".into(),
        ))
    }
}

impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Serde反序列化器实现
pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    fn take_bytes(&mut self, len: usize) -> &[u8] {
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        bytes
    }

    // - 如果这个 0 之后的值是 255，说明是原始字符串中的 0，则继续解析
    // - 如果这个 0 之后的值是 0，说明是字符串的结尾
    fn next_bytes(&mut self) -> Result<Vec<u8>> {
        let mut res = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let i = loop {
            match iter.next() {
                Some((_, 0)) => match iter.next() {
                    Some((i, 0)) => break i + 1,
                    Some((_, 255)) => res.push(0),
                    _ => return Err(Error::Internal("unexpected input".into())),
                },
                Some((_, b)) => res.push(*b),
                _ => return Err(Error::Internal("unexpected input".into())),
            }
        };
        self.input = &self.input[i..];
        Ok(res)
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        // 根据第一个字节判断类型，做简单实现
        if self.input.is_empty() {
            return Err(Error::Internal("Unexpected end of input".into()));
        }

        // 检查第一个字节
        match self.input[0] {
            0 => self.deserialize_bool(visitor),
            _ => self.deserialize_bytes(visitor),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let v = self.take_bytes(1)[0];
        visitor.visit_bool(v != 0)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.len() < 1 {
            return Err(Error::Internal("Input too short for i8".into()));
        }
        let bytes = self.take_bytes(1);
        let v = i8::from_be_bytes([bytes[0]]);
        visitor.visit_i8(v)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.len() < 2 {
            return Err(Error::Internal("Input too short for i16".into()));
        }
        let bytes = self.take_bytes(2);
        let v = i16::from_be_bytes([bytes[0], bytes[1]]);
        visitor.visit_i16(v)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.len() < 4 {
            return Err(Error::Internal("Input too short for i32".into()));
        }
        let bytes = self.take_bytes(4);
        let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        visitor.visit_i32(v)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(8);
        let v = i64::from_be_bytes(bytes.try_into()?);
        visitor.visit_i64(v)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.is_empty() {
            return Err(Error::Internal("Input too short for u8".into()));
        }
        let byte = self.take_bytes(1)[0];
        visitor.visit_u8(byte)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.len() < 2 {
            return Err(Error::Internal("Input too short for u16".into()));
        }
        let bytes = self.take_bytes(2);
        let v = u16::from_be_bytes([bytes[0], bytes[1]]);
        visitor.visit_u16(v)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.len() < 4 {
            return Err(Error::Internal("Input too short for u32".into()));
        }
        let bytes = self.take_bytes(4);
        let v = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        visitor.visit_u32(v)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(8);
        let v = u64::from_be_bytes(bytes.try_into()?);
        visitor.visit_u64(v)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.len() < 4 {
            return Err(Error::Internal("Input too short for f32".into()));
        }
        let bytes = self.take_bytes(4);
        let v = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        visitor.visit_f32(v)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(8);
        let v = f64::from_be_bytes(bytes.try_into()?);
        visitor.visit_f64(v)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        // 先读取一个字节序列，然后尝试转换为字符
        let bytes = self.next_bytes()?;
        let s = String::from_utf8(bytes)?;

        if s.chars().count() != 1 {
            return Err(Error::Internal("Expected a single character".into()));
        }

        visitor.visit_char(s.chars().next().unwrap())
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.next_bytes()?;
        visitor.visit_str(&String::from_utf8(bytes)?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        // 字符串和str一样处理
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_bytes(&self.next_bytes()?)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_byte_buf(self.next_bytes()?)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.is_empty() {
            return Err(Error::Internal("Input too short for option".into()));
        }

        // 读取标记字节
        let tag = self.take_bytes(1)[0];
        match tag {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            _ => Err(Error::Internal(format!("Invalid option tag: {}", tag))),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        // 我们的实现不支持Map，但提供一个空序列以兼容一些简单场景
        visitor.visit_seq(self)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        // 将结构体视为序列，按顺序读取字段
        visitor.visit_seq(self)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        // 标识符一般是字符串
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        // 忽略值，但要消耗输入
        self.deserialize_any(visitor)
    }
}

impl<'de, 'a> de::SeqAccess<'de> for Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self).map(Some)
    }
}

impl<'de, 'a> de::EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let index = self.take_bytes(1)[0] as u32;
        let varint_index: Result<_> = seed.deserialize(index.into_deserializer());
        Ok((varint_index?, self))
    }
}

impl<'de, 'a> de::VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

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

    // 测试基本类型的序列化/反序列化
    #[test]
    fn test_primitives_serde() {
        // 测试基本类型
        let values = [
            serialize_key(&true).unwrap(),
            serialize_key(&42i8).unwrap(),
            serialize_key(&12345i16).unwrap(),
            serialize_key(&1234567i32).unwrap(),
            serialize_key(&123456789123i64).unwrap(),
            serialize_key(&42u8).unwrap(),
            serialize_key(&12345u16).unwrap(),
            serialize_key(&1234567u32).unwrap(),
            serialize_key(&123456789123u64).unwrap(),
            serialize_key(&3.14f32).unwrap(),
            serialize_key(&3.14159265359f64).unwrap(),
            serialize_key(&'A').unwrap(),
            serialize_key(&"Hello, world!").unwrap(),
        ];

        // 确保每个值都不是空的
        for (i, val) in values.iter().enumerate() {
            assert!(!val.is_empty(), "Value at index {} is empty", i);
        }

        // 反序列化并验证
        assert_eq!(true, deserialize_key::<bool>(&values[0]).unwrap());
        assert_eq!(42i8, deserialize_key::<i8>(&values[1]).unwrap());
        assert_eq!(12345i16, deserialize_key::<i16>(&values[2]).unwrap());
        assert_eq!(1234567i32, deserialize_key::<i32>(&values[3]).unwrap());
        assert_eq!(123456789123i64, deserialize_key::<i64>(&values[4]).unwrap());
        assert_eq!(42u8, deserialize_key::<u8>(&values[5]).unwrap());
        assert_eq!(12345u16, deserialize_key::<u16>(&values[6]).unwrap());
        assert_eq!(1234567u32, deserialize_key::<u32>(&values[7]).unwrap());
        assert_eq!(123456789123u64, deserialize_key::<u64>(&values[8]).unwrap());

        // 浮点数需要特殊处理，因为直接比较可能不精确
        let f32_val = deserialize_key::<f32>(&values[9]).unwrap();
        assert!((f32_val - 3.14f32).abs() < f32::EPSILON);

        let f64_val = deserialize_key::<f64>(&values[10]).unwrap();
        assert!((f64_val - 3.14159265359f64).abs() < f64::EPSILON);

        assert_eq!('A', deserialize_key::<char>(&values[11]).unwrap());
        assert_eq!(
            "Hello, world!",
            deserialize_key::<String>(&values[12]).unwrap()
        );
    }

    // 测试特殊情况：字节序列中含有0
    #[test]
    fn test_bytes_with_zeros() {
        let bytes_with_zeros = vec![97, 98, 0, 99];
        let serialized = serialize_key(&bytes_with_zeros).unwrap();
        let deserialized: Vec<u8> = deserialize_key(&serialized).unwrap();
        assert_eq!(bytes_with_zeros, deserialized);

        // 更极端的情况：多个连续的0
        let bytes_with_multiple_zeros = vec![97, 0, 0, 99];
        let serialized = serialize_key(&bytes_with_multiple_zeros).unwrap();
        let deserialized: Vec<u8> = deserialize_key(&serialized).unwrap();
        assert_eq!(bytes_with_multiple_zeros, deserialized);
    }

    // 测试Option类型
    #[test]
    fn test_option() {
        let some_value: Option<i32> = Some(42);
        let none_value: Option<i32> = None;

        let serialized_some = serialize_key(&some_value).unwrap();
        let serialized_none = serialize_key(&none_value).unwrap();

        let deserialized_some: Option<i32> = deserialize_key(&serialized_some).unwrap();
        let deserialized_none: Option<i32> = deserialize_key(&serialized_none).unwrap();

        assert_eq!(some_value, deserialized_some);
        assert_eq!(none_value, deserialized_none);
    }

    // 测试序列类型
    #[test]
    fn test_sequence() {
        let seq = vec![1, 2, 3, 4, 5];
        let serialized = serialize_key(&seq).unwrap();
        let deserialized: Vec<i32> = deserialize_key(&serialized).unwrap();
        assert_eq!(seq, deserialized);
    }

    // 测试简单枚举
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum TestEnum {
        A,
        B(i32),
        C(String, i32),
    }

    #[test]
    fn test_enum() {
        let values = [
            TestEnum::A,
            TestEnum::B(42),
            TestEnum::C("hello".to_string(), 123),
        ];

        for value in values.iter() {
            let serialized = serialize_key(value).unwrap();
            let deserialized: TestEnum = deserialize_key(&serialized).unwrap();
            assert_eq!(*value, deserialized);
        }
    }
}

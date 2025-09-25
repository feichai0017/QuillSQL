use crate::buffer::PAGE_SIZE;
use crate::error::QuillSQLResult;
use crate::storage::codec::{CommonCodec, DecodedData};
use crate::storage::page::{FreelistPage, FreelistPageHeader};

pub struct FreelistPageHeaderCodec;

impl FreelistPageHeaderCodec {
    pub fn encode(header: &FreelistPageHeader) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(CommonCodec::encode_u32(header.next_page_id));
        bytes.extend(CommonCodec::encode_u32(header.current_size));
        bytes.extend(CommonCodec::encode_u32(header.max_size));
        bytes
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<FreelistPageHeader>> {
        let mut left_bytes = bytes;

        let (next_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (current_size, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (max_size, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        Ok((
            FreelistPageHeader {
                next_page_id,
                current_size,
                max_size,
            },
            bytes.len() - left_bytes.len(),
        ))
    }
}

pub struct FreelistPageCodec;

impl FreelistPageCodec {
    pub fn encode(page: &FreelistPage) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(FreelistPageHeaderCodec::encode(&page.header));
        for i in 0..page.header.current_size {
            bytes.extend(CommonCodec::encode_u32(page.array[i as usize]))
        }
        // make sure length of bytes is BUSTUBX_PAGE_SIZE
        assert!(bytes.len() <= PAGE_SIZE);
        bytes.extend(vec![0; PAGE_SIZE - bytes.len()]);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<FreelistPage>> {
        let mut left_bytes = bytes;

        let (header, offset) = FreelistPageHeaderCodec::decode(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let mut array = Vec::new();
        for _ in 0..header.current_size {
            let (page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
            left_bytes = &left_bytes[offset..];
            array.push(page_id);
        }

        Ok((FreelistPage { header, array }, PAGE_SIZE))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::codec::FreelistPageCodec;
    use crate::storage::page::{FreelistPage, FreelistPageHeader, FREELIST_PAGE_MAX_SIZE};

    #[test]
    fn freelist_page_codec() {
        let page = FreelistPage {
            header: FreelistPageHeader {
                next_page_id: 1,
                current_size: 3,
                max_size: *FREELIST_PAGE_MAX_SIZE as u32,
            },
            array: vec![5, 6, 8],
        };
        let (new_page, _) = FreelistPageCodec::decode(&FreelistPageCodec::encode(&page)).unwrap();
        assert_eq!(page, new_page);
    }
}

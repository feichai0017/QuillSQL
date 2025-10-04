use crate::buffer::PageId;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::Lsn;

#[derive(Debug, Clone)]
pub struct PageWritePayload {
    pub page_id: PageId,
    pub prev_page_lsn: Lsn,
    pub page_image: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PageDeltaPayload {
    pub page_id: PageId,
    pub prev_page_lsn: Lsn,
    pub offset: u16,
    pub data: Vec<u8>,
}

pub fn encode_page_write(body: &PageWritePayload) -> Vec<u8> {
    // Page/PageWrite (rmid=Page, info=0)
    // body: page_id(4) + prev_page_lsn(8) + image_len(4) + page_image[]
    let mut buf = Vec::with_capacity(4 + 8 + 4 + body.page_image.len());
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.prev_page_lsn.to_le_bytes());
    buf.extend_from_slice(&(body.page_image.len() as u32).to_le_bytes());
    buf.extend_from_slice(&body.page_image);
    buf
}

pub fn encode_page_delta(body: &PageDeltaPayload) -> Vec<u8> {
    // Page/PageDelta (rmid=Page, info=1)
    // body: page_id(4) + prev_page_lsn(8) + offset(2) + len(4) + data[]
    let mut buf = Vec::with_capacity(4 + 8 + 2 + 4 + body.data.len());
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.prev_page_lsn.to_le_bytes());
    buf.extend_from_slice(&body.offset.to_le_bytes());
    buf.extend_from_slice(&(body.data.len() as u32).to_le_bytes());
    buf.extend_from_slice(&body.data);
    buf
}

pub fn decode_page_write(bytes: &[u8]) -> QuillSQLResult<PageWritePayload> {
    if bytes.len() < 4 + 8 + 4 {
        return Err(QuillSQLError::Internal(
            "PageWrite payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as PageId;
    let prev_page_lsn = u64::from_le_bytes(bytes[4..12].try_into().unwrap()) as Lsn;
    let image_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    if bytes.len() != 16 + image_len {
        return Err(QuillSQLError::Internal(
            "PageWrite payload length mismatch".to_string(),
        ));
    }
    let page_image = bytes[16..].to_vec();
    Ok(PageWritePayload {
        page_id,
        prev_page_lsn,
        page_image,
    })
}

pub fn decode_page_delta(bytes: &[u8]) -> QuillSQLResult<PageDeltaPayload> {
    if bytes.len() < 4 + 8 + 2 + 4 {
        return Err(QuillSQLError::Internal(
            "PageDelta payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as PageId;
    let prev_page_lsn = u64::from_le_bytes(bytes[4..12].try_into().unwrap()) as Lsn;
    let offset = u16::from_le_bytes(bytes[12..14].try_into().unwrap());
    let data_len = u32::from_le_bytes(bytes[14..18].try_into().unwrap()) as usize;
    if bytes.len() != 18 + data_len {
        return Err(QuillSQLError::Internal(
            "PageDelta payload length mismatch".to_string(),
        ));
    }
    let data = bytes[18..].to_vec();
    Ok(PageDeltaPayload {
        page_id,
        prev_page_lsn,
        offset,
        data,
    })
}

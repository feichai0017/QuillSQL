use std::collections::VecDeque;

use super::record::WalRecord;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::Lsn;

pub const WAL_PAGE_SIZE: usize = 4096;
const WAL_PAGE_MAGIC: u32 = 0x5157_5047; // "QWPG"
const WAL_PAGE_VERSION: u16 = 1;

const WAL_PAGE_HEADER_LEN: usize = 4 + 2 + 2 + 8 + 2 + 2;
const WAL_PAGE_SLOT_LEN: usize = 8;

/// Header for a WalPage, containing metadata about the page itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalPageHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
    pub prev_page_lsn: Lsn,
    pub payload_size: u16,
    pub slot_count: u16,
}

impl WalPageHeader {
    fn encode(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.prev_page_lsn.to_le_bytes());
        buf[16..18].copy_from_slice(&self.payload_size.to_le_bytes());
        buf[18..20].copy_from_slice(&self.slot_count.to_le_bytes());
    }

    fn decode(bytes: &[u8]) -> QuillSQLResult<Self> {
        if bytes.len() < WAL_PAGE_HEADER_LEN {
            return Err(QuillSQLError::Internal(
                "WAL page truncated before header".to_string(),
            ));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        let prev_page_lsn = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let payload_size = u16::from_le_bytes(bytes[16..18].try_into().unwrap());
        let slot_count = u16::from_le_bytes(bytes[18..20].try_into().unwrap());
        Ok(Self {
            magic,
            version,
            flags,
            prev_page_lsn,
            payload_size,
            slot_count,
        })
    }
}

/// Describes whether a fragment in a WalPage is a self-contained record
/// or part of a larger record that spans multiple pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalPageFragmentKind {
    Complete,
    Start,
    Middle,
    End,
}

impl WalPageFragmentKind {
    fn to_byte(self) -> u8 {
        match self {
            WalPageFragmentKind::Complete => 0,
            WalPageFragmentKind::Start => 1,
            WalPageFragmentKind::Middle => 2,
            WalPageFragmentKind::End => 3,
        }
    }

    fn from_byte(value: u8) -> QuillSQLResult<Self> {
        match value {
            0 => Ok(WalPageFragmentKind::Complete),
            1 => Ok(WalPageFragmentKind::Start),
            2 => Ok(WalPageFragmentKind::Middle),
            3 => Ok(WalPageFragmentKind::End),
            other => Err(QuillSQLError::Internal(format!(
                "Unknown WAL page fragment kind: {}",
                other
            ))),
        }
    }
}

/// A slot in the WalPage's slot directory. It points to a specific
/// fragment within the page's payload area.
#[derive(Debug, Clone, Copy)]
pub struct WalPageSlot {
    pub offset: u16,
    pub len: u16,
    pub kind: WalPageFragmentKind,
}

impl WalPageSlot {
    fn encode(&self) -> [u8; WAL_PAGE_SLOT_LEN] {
        let mut buf = [0u8; WAL_PAGE_SLOT_LEN];
        buf[0..2].copy_from_slice(&self.offset.to_le_bytes());
        buf[2..4].copy_from_slice(&self.len.to_le_bytes());
        buf[4] = self.kind.to_byte();
        buf
    }

    fn decode(bytes: &[u8]) -> QuillSQLResult<Self> {
        if bytes.len() < WAL_PAGE_SLOT_LEN {
            return Err(QuillSQLError::Internal(
                "WAL page truncated before slot".to_string(),
            ));
        }
        let offset = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
        let len = u16::from_le_bytes(bytes[2..4].try_into().unwrap());
        let kind = WalPageFragmentKind::from_byte(bytes[4])?;
        Ok(Self { offset, len, kind })
    }
}

/// Represents a WalRecord that is being carried over to the next WalPage
/// because it could not fit entirely in the previous one.
#[derive(Clone)]
pub struct WalFrameContinuation {
    pub record: WalRecord,
    pub offset: usize,
}

impl WalFrameContinuation {
    fn remaining(&self) -> usize {
        self.record.payload.len().saturating_sub(self.offset)
    }
}

/// A WalPage is a fixed-size (e.g., 4KB) block within a WAL segment file.
/// It contains a header, a payload area for raw data, and a slot directory
/// that describes the fragments packed into the payload.
///
/// The payload and slot directory grow towards each other from opposite ends of the page.
///
/// WalPage On-disk Layout (4KB):
/// ------------------------------------------------------------------------------------------
/// | WalPageHeader | Payload Data (grows forward) | ... Free Space ... | Slot Array (grows backward) |
/// ------------------------------------------------------------------------------------------
///
/// WalPageHeader (20 bytes):
/// ------------------------------------------------------------------------------------------
/// | Magic (u32) | Ver (u16) | Flags (u16) | PrevPageLSN (u64) | PayloadSize (u16) | SlotCount (u16) |
/// ------------------------------------------------------------------------------------------
///
/// WalPageSlot (8 bytes):
/// -----------------------------------------------------------------
/// | Offset (u16) | Length (u16) | Kind (u8) | Reserved (3) |
/// -----------------------------------------------------------------
/// - Offset/Length: Point to a fragment within this page's Payload Data.
/// - Kind: Indicates if the fragment is a Complete, Start, Middle, or End piece of a WalFrame.
#[derive(Clone)]
pub struct WalPage {
    header: WalPageHeader,
    payload: Vec<u8>,
    slots: Vec<WalPageSlot>,
    full: bool,
    continuation: Option<WalFrameContinuation>,
    last_end_lsn: Option<Lsn>,
}

impl WalPage {
    pub fn pack_frames(
        prev_page_lsn: Lsn,
        frames: Vec<WalRecord>,
        carry: Option<WalFrameContinuation>,
    ) -> (Self, Vec<WalRecord>, Option<WalFrameContinuation>) {
        let queue: VecDeque<WalRecord> = frames.into();
        let (payload, slots, leftover, continuation, last_end_lsn, full) =
            Self::fill_page(Vec::new(), Vec::new(), queue, carry, None);

        let header = WalPageHeader {
            magic: WAL_PAGE_MAGIC,
            version: WAL_PAGE_VERSION,
            flags: 0,
            prev_page_lsn,
            payload_size: payload.len() as u16,
            slot_count: slots.len() as u16,
        };

        (
            Self {
                header,
                payload,
                slots,
                full,
                continuation: continuation.clone(),
                last_end_lsn,
            },
            leftover,
            continuation,
        )
    }

    pub fn continue_pack(
        mut self,
        frames: Vec<WalRecord>,
    ) -> (Self, Vec<WalRecord>, Option<WalFrameContinuation>) {
        let queue: VecDeque<WalRecord> = frames.into();
        let (payload, slots, leftover, continuation, last_end_lsn, full) = Self::fill_page(
            self.payload,
            self.slots,
            queue,
            self.continuation.take(),
            self.last_end_lsn,
        );

        self.payload = payload;
        self.slots = slots;
        self.continuation = continuation.clone();
        self.last_end_lsn = last_end_lsn;
        self.full = full;
        self.header.payload_size = self.payload.len() as u16;
        self.header.slot_count = self.slots.len() as u16;

        (self, leftover, continuation)
    }

    fn fill_page(
        mut payload: Vec<u8>,
        mut slots: Vec<WalPageSlot>,
        mut queue: VecDeque<WalRecord>,
        mut continuation: Option<WalFrameContinuation>,
        mut last_end_lsn: Option<Lsn>,
    ) -> (
        Vec<u8>,
        Vec<WalPageSlot>,
        Vec<WalRecord>,
        Option<WalFrameContinuation>,
        Option<Lsn>,
        bool,
    ) {
        loop {
            if continuation.is_none() && queue.is_empty() {
                break;
            }

            let available = Self::available_bytes(payload.len(), slots.len());
            if available == 0 {
                break;
            }

            if let Some(mut cont) = continuation.take() {
                if cont.remaining() == 0 {
                    last_end_lsn = Some(cont.record.end_lsn);
                    continue;
                }
                let take = available.min(cont.remaining());
                if take == 0 {
                    continuation = Some(cont);
                    break;
                }
                let start = payload.len();
                payload.extend_from_slice(&cont.record.payload[cont.offset..cont.offset + take]);
                let kind = if cont.offset == 0 {
                    if cont.offset + take == cont.record.payload.len() {
                        WalPageFragmentKind::Complete
                    } else {
                        WalPageFragmentKind::Start
                    }
                } else if cont.offset + take == cont.record.payload.len() {
                    WalPageFragmentKind::End
                } else {
                    WalPageFragmentKind::Middle
                };
                slots.push(WalPageSlot {
                    offset: start as u16,
                    len: take as u16,
                    kind,
                });
                cont.offset += take;
                if cont.offset == cont.record.payload.len() {
                    last_end_lsn = Some(cont.record.end_lsn);
                    continuation = None;
                } else {
                    continuation = Some(cont);
                }
                continue;
            }

            if let Some(record) = queue.pop_front() {
                continuation = Some(WalFrameContinuation { record, offset: 0 });
                continue;
            }
        }

        let leftover: Vec<WalRecord> = queue.into_iter().collect();
        let full = Self::available_for_next(payload.len(), slots.len()) == 0;

        (payload, slots, leftover, continuation, last_end_lsn, full)
    }

    pub fn unpack_frames(bytes: &[u8]) -> QuillSQLResult<Self> {
        if bytes.len() < WAL_PAGE_SIZE {
            return Err(QuillSQLError::Internal(
                "WAL page truncated before full page".to_string(),
            ));
        }
        if bytes.iter().all(|&b| b == 0) {
            return Ok(Self::empty());
        }

        let header = WalPageHeader::decode(&bytes[..WAL_PAGE_HEADER_LEN])?;
        if header.magic != WAL_PAGE_MAGIC {
            return Err(QuillSQLError::Internal(format!(
                "Invalid WAL page magic: {:x}",
                header.magic
            )));
        }
        if header.version != WAL_PAGE_VERSION {
            return Err(QuillSQLError::Internal(format!(
                "Unsupported WAL page version: {}",
                header.version
            )));
        }

        let payload_end = WAL_PAGE_HEADER_LEN + header.payload_size as usize;
        if payload_end > WAL_PAGE_SIZE {
            return Err(QuillSQLError::Internal(
                "WAL page payload exceeds page size".to_string(),
            ));
        }
        let dir_start = WAL_PAGE_SIZE
            .checked_sub(header.slot_count as usize * WAL_PAGE_SLOT_LEN)
            .ok_or_else(|| {
                QuillSQLError::Internal("WAL page directory exceeds page size".to_string())
            })?;
        if dir_start < payload_end {
            return Err(QuillSQLError::Internal(
                "WAL page directory overlaps payload".to_string(),
            ));
        }

        let payload = bytes[WAL_PAGE_HEADER_LEN..payload_end].to_vec();
        let mut slots = Vec::with_capacity(header.slot_count as usize);
        let mut cursor = dir_start;
        for _ in 0..header.slot_count {
            let slot = WalPageSlot::decode(&bytes[cursor..cursor + WAL_PAGE_SLOT_LEN])?;
            if slot.offset as usize + slot.len as usize > payload.len() {
                return Err(QuillSQLError::Internal(
                    "WAL page slot exceeds payload".to_string(),
                ));
            }
            slots.push(slot);
            cursor += WAL_PAGE_SLOT_LEN;
        }

        let full = Self::available_for_next(payload.len(), slots.len()) == 0;
        Ok(Self {
            header,
            payload,
            slots,
            full,
            continuation: None,
            last_end_lsn: None,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; WAL_PAGE_SIZE];
        self.header.encode(&mut buf[..WAL_PAGE_HEADER_LEN]);
        buf[WAL_PAGE_HEADER_LEN..WAL_PAGE_HEADER_LEN + self.payload.len()]
            .copy_from_slice(&self.payload);
        let dir_start = WAL_PAGE_SIZE - self.slots.len() * WAL_PAGE_SLOT_LEN;
        let mut cursor = dir_start;
        for slot in &self.slots {
            let encoded = slot.encode();
            buf[cursor..cursor + WAL_PAGE_SLOT_LEN].copy_from_slice(&encoded);
            cursor += WAL_PAGE_SLOT_LEN;
        }
        buf
    }

    pub fn fragments(&self) -> &[WalPageSlot] {
        &self.slots
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn is_full(&self) -> bool {
        self.full
    }

    pub fn has_payload(&self) -> bool {
        !self.payload.is_empty() || !self.slots.is_empty()
    }

    pub fn last_end_lsn(&self) -> Option<Lsn> {
        self.last_end_lsn
    }

    pub fn continuation(&self) -> Option<&WalFrameContinuation> {
        self.continuation.as_ref()
    }

    pub fn prev_page_lsn(&self) -> Lsn {
        self.header.prev_page_lsn
    }

    fn empty() -> Self {
        Self {
            header: WalPageHeader {
                magic: WAL_PAGE_MAGIC,
                version: WAL_PAGE_VERSION,
                flags: 0,
                prev_page_lsn: 0,
                payload_size: 0,
                slot_count: 0,
            },
            payload: Vec::new(),
            slots: Vec::new(),
            full: false,
            continuation: None,
            last_end_lsn: None,
        }
    }

    fn available_bytes(payload_len: usize, slot_count: usize) -> usize {
        WAL_PAGE_SIZE
            .saturating_sub(WAL_PAGE_HEADER_LEN)
            .saturating_sub(payload_len)
            .saturating_sub((slot_count + 1) * WAL_PAGE_SLOT_LEN)
    }

    fn available_for_next(payload_len: usize, slot_count: usize) -> usize {
        WAL_PAGE_SIZE
            .saturating_sub(WAL_PAGE_HEADER_LEN)
            .saturating_sub(payload_len)
            .saturating_sub((slot_count + 1) * WAL_PAGE_SLOT_LEN)
    }
}

impl Default for WalPage {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::wal::codec::{decode_frame, encode_frame};
    use crate::recovery::wal_record::{
        PageWritePayload, TransactionPayload, TransactionRecordKind, WalFrame, WalRecordPayload,
        WAL_CRC_LEN, WAL_HEADER_LEN,
    };
    use bytes::Bytes;

    fn make_transaction_record(start: Lsn, prev: Lsn, txn: u64) -> WalRecord {
        let payload = WalRecordPayload::Transaction(TransactionPayload {
            marker: TransactionRecordKind::Begin,
            txn_id: txn,
        });
        build_record(start, prev, &payload)
    }

    fn build_record(start: Lsn, prev: Lsn, payload: &WalRecordPayload) -> WalRecord {
        let frame = encode_frame(start, prev, payload);
        let end = start + frame.len() as u64;
        WalRecord {
            start_lsn: start,
            end_lsn: end,
            payload: Bytes::from(frame),
        }
    }

    fn decode_pages(pages: &[WalPage]) -> Vec<WalFrame> {
        let mut frames = Vec::new();
        let mut buffer = Vec::new();
        for page in pages {
            for slot in page.fragments() {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                let fragment = &page.payload()[start..end];
                match slot.kind {
                    WalPageFragmentKind::Complete => {
                        buffer.clear();
                        let (frame, _) = decode_frame(fragment).expect("frame");
                        frames.push(frame);
                    }
                    WalPageFragmentKind::Start => {
                        buffer.clear();
                        buffer.extend_from_slice(fragment);
                    }
                    WalPageFragmentKind::Middle => {
                        assert!(!buffer.is_empty());
                        buffer.extend_from_slice(fragment);
                    }
                    WalPageFragmentKind::End => {
                        assert!(!buffer.is_empty());
                        buffer.extend_from_slice(fragment);
                        let (frame, _) = decode_frame(&buffer).expect("frame");
                        frames.push(frame);
                        buffer.clear();
                    }
                }
            }
        }
        frames
    }

    #[test]
    fn pack_single_page_roundtrip() {
        let mut records = Vec::new();
        let mut start = 0;
        let mut prev = 0;
        for txn in 0..8 {
            let record = make_transaction_record(start, prev, txn);
            prev = record.start_lsn;
            start = record.end_lsn;
            records.push(record);
        }

        let (page, leftover, carry) = WalPage::pack_frames(0, records.clone(), None);
        assert!(leftover.is_empty());
        assert!(carry.is_none());
        assert!(page.has_payload());

        let bytes = page.to_bytes();
        let decoded = WalPage::unpack_frames(&bytes).expect("unpack");
        let frames = decode_pages(&[decoded]);
        assert_eq!(frames.len(), records.len());
        for (frame, record) in frames.iter().zip(records.iter()) {
            assert_eq!(frame.lsn, record.start_lsn);
        }
    }

    #[test]
    fn pack_multiple_pages() {
        let mut records = Vec::new();
        let mut start = 0;
        let mut prev = 0;
        // Enough records to span multiple pages
        for txn in 0..128 {
            let record = make_transaction_record(start, prev, txn);
            prev = record.start_lsn;
            start = record.end_lsn;
            records.push(record);
        }

        let mut queue = records.clone();
        let mut prev_page_lsn = 0;
        let mut carry = None;
        let mut pages = Vec::new();
        while !queue.is_empty() || carry.is_some() {
            let (page, leftover, next) = WalPage::pack_frames(prev_page_lsn, queue, carry);
            if page.has_payload() {
                prev_page_lsn = page.last_end_lsn().unwrap_or(prev_page_lsn);
                pages.push(page);
            }
            queue = leftover;
            carry = next;
        }

        assert!(pages.len() > 1);
        let frames = decode_pages(&pages);
        assert_eq!(frames.len(), records.len());
        for (frame, record) in frames.iter().zip(records.iter()) {
            assert_eq!(frame.lsn, record.start_lsn);
        }
    }

    #[test]
    fn pack_cross_page_frame() {
        let page_image = vec![7u8; 4096];
        let payload = WalRecordPayload::PageWrite(PageWritePayload {
            page_id: 1,
            prev_page_lsn: 0,
            page_image,
        });
        let record = build_record(0, 0, &payload);

        let mut pages = Vec::new();
        let mut queue = vec![record.clone()];
        let mut prev_page_lsn = 0;
        let mut carry = None;
        while !queue.is_empty() || carry.is_some() {
            let (page, leftover, next) = WalPage::pack_frames(prev_page_lsn, queue, carry);
            assert!(page.has_payload());
            prev_page_lsn = page.last_end_lsn().unwrap_or(prev_page_lsn);
            pages.push(page);
            queue = leftover;
            carry = next;
        }

        assert!(pages.len() >= 2);
        let frames = decode_pages(&pages);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].lsn, record.start_lsn);
        assert_eq!(
            frames[0].body.len(),
            record.payload.len() - WAL_HEADER_LEN - WAL_CRC_LEN
        );
    }
}

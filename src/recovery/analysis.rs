use crate::error::QuillSQLResult;
use crate::recovery::control_file::ControlFileSnapshot;
use crate::recovery::wal_record::{CheckpointPayload, WalFrame, WalRecordPayload};
use crate::recovery::Lsn;

#[derive(Debug, Default, Clone)]
pub struct AnalysisResult {
    pub start_lsn: Lsn,
    pub has_frames: bool,
}

pub struct AnalysisPass {
    latest: Option<(Lsn, CheckpointPayload)>,
    snapshot: Option<ControlFileSnapshot>,
    has_frames: bool,
}

impl AnalysisPass {
    pub fn new(snapshot: Option<ControlFileSnapshot>) -> Self {
        Self {
            latest: None,
            snapshot,
            has_frames: false,
        }
    }

    pub fn observe(&mut self, frame: &WalFrame) {
        self.has_frames = true;
        if let WalRecordPayload::Checkpoint(payload) = &frame.payload {
            self.latest = Some((frame.lsn, payload.clone()));
        }
    }

    pub fn finalize(self) -> QuillSQLResult<AnalysisResult> {
        let start_lsn = if let Some((checkpoint_lsn, payload)) = &self.latest {
            self.snapshot
                .map(|snap| snap.checkpoint_redo_start)
                .filter(|redo| *redo >= payload.last_lsn && *redo <= *checkpoint_lsn)
                .unwrap_or_else(|| {
                    payload
                        .dpt
                        .iter()
                        .map(|(_, lsn)| *lsn)
                        .min()
                        .unwrap_or(payload.last_lsn)
                })
        } else {
            0
        };

        Ok(AnalysisResult {
            start_lsn,
            has_frames: self.has_frames,
        })
    }
}

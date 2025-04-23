pub mod bitcask;
pub mod cache;
pub mod datafile;
pub mod indexfile;
pub mod keydir;

pub const REMOVE_TOMBSTONE: &[u8] = b"%_%_%_%<!(R|E|M|O|V|E|D)!>%_%_%_%_";
pub static DATA_FILE_GLOB_FORMAT: &str = "data.*";

pub fn data_file_format(id: u128) -> String {
    format!("data.{}", id)
}
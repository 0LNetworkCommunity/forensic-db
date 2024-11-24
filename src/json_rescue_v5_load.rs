use crate::json_rescue_v5_extract::{
    decompress_to_temppath, extract_v5_json_rescue, list_all_json_files,
};
use anyhow::Result;
use std::path::Path;

/// from a tgz file decompress all the .json files in archive
/// and then read into the warehouse record format
pub fn e2e_decompress_and_extract(tgz_file: &Path) -> Result<()> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    for j in json_vec {
        if let Ok((r, _e)) = extract_v5_json_rescue(&j) {
            dbg!(&r.len());
        }
    }

    Ok(())
}

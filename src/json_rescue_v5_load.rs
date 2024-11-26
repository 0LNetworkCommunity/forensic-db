use crate::{
    json_rescue_v5_extract::{
        decompress_to_temppath, extract_v5_json_rescue, list_all_json_files, list_all_tgz_archives,
    },
    load_tx_cypher::tx_batch,
};
use anyhow::Result;
use log::{error, info};
use neo4rs::Graph;
use std::path::Path;

/// from a tgz file decompress all the .json files in archive
/// and then read into the warehouse record format
pub async fn e2e_decompress_and_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    let mut transactions = 0u64;
    for j in json_vec {
        if let Ok((r, _e)) = extract_v5_json_rescue(&j) {
            tx_batch(&r, pool, 250, j.to_str().unwrap()).await?;
            transactions += r.len() as u64;
        }
    }

    info!("V5 transactions processed: {}", transactions);

    Ok(transactions)
}

pub async fn rip(start_dir: &Path, pool: &Graph) -> Result<u64> {
    let tgz_list = list_all_tgz_archives(start_dir)?;
    let mut txs = 0u64;
    for p in tgz_list.iter() {
        match e2e_decompress_and_extract(p, pool).await {
            Ok(t) => txs += t,
            Err(e) => {
                error!(
                    "could not load archive: {}, error: {}",
                    p.display(),
                    e.to_string()
                );
            }
        }
    }
    Ok(txs)
}

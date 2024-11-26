use crate::{
    json_rescue_v5_extract::{
        decompress_to_temppath, extract_v5_json_rescue, list_all_json_files, list_all_tgz_archives,
    },
    load_tx_cypher::tx_batch,
    schema_transaction::WarehouseTxMaster,
};
use anyhow::Result;
use log::{error, info};
use neo4rs::Graph;
use std::path::Path;

/// How many records to read from the archives before attempting insert
static LOAD_QUEUE_SIZE: usize = 1000;
/// When we attempt insert, the chunks of txs that go in to each query
static QUERY_BATCH_SIZE: usize = 250;

/// from a tgz file decompress all the .json files in archive
/// and then read into the warehouse record format
pub async fn decompress_and_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    let mut created_count = 0u64;

    // fill to BATCH_SIZE before attempting insert.
    // many files may only have a handful of user txs,
    // so individual files may have far fewer than BATCH_SIZE.
    let mut queue: Vec<WarehouseTxMaster> = vec![];

    for j in json_vec {
        if let Ok((mut r, _e)) = extract_v5_json_rescue(&j) {
            queue.append(&mut r);
        }

        if queue.len() >= LOAD_QUEUE_SIZE {
            let drain: Vec<WarehouseTxMaster> = std::mem::take(&mut queue);
            let res = tx_batch(&drain, pool, QUERY_BATCH_SIZE, j.to_str().unwrap()).await?;
            created_count += res.created_tx as u64;
        }
    }

    info!("V5 transactions processed: {}", created_count);

    Ok(created_count)
}

pub async fn rip(start_dir: &Path, pool: &Graph) -> Result<u64> {
    let tgz_list = list_all_tgz_archives(start_dir)?;
    info!("tgz archives found: {}", tgz_list.len());
    let mut txs = 0u64;
    for p in tgz_list.iter() {
        match decompress_and_extract(p, pool).await {
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

use crate::{
    extract_transactions::extract_current_transactions,
    load_tx_cypher::{self, BatchTxReturn},
    queue::{self, clear_queue, push_queue_from_archive_map},
    scan::{ArchiveMap, ManifestInfo},
};

use anyhow::Result;
use log::info;
use neo4rs::Graph;

/// takes all the archives from a map, and tries to load them sequentially
pub async fn ingest_all(archive_map: &ArchiveMap, pool: &Graph, force_queue: bool) -> Result<()> {
    if force_queue {
        clear_queue(pool).await?;
        push_queue_from_archive_map(archive_map, pool).await?;
    }

    // get queue of any batch which has any incomplete batches
    let pending = queue::get_queued(pool).await?;
    info!("pending archives: {}", pending.len());

    for (_p, m) in archive_map.0.iter() {
        println!(
            "\nProcessing: {:?} with archive: {}",
            m.contents,
            m.archive_dir.display()
        );

        if pending.contains(&m.archive_id) {
            info!("archive queued: {}", m.archive_dir.display());
            let batch_tx_return = try_load_one_archive(m, pool).await?;
            println!("SUCCESS: {}", batch_tx_return);
        } else {
            info!(
                "archive complete (or not in queue): {}",
                m.archive_dir.display()
            );
        }
    }

    Ok(())
}

pub async fn try_load_one_archive(man: &ManifestInfo, pool: &Graph) -> Result<BatchTxReturn> {
    let mut all_results = BatchTxReturn::new();
    match man.contents {
        crate::scan::BundleContent::Unknown => todo!(),
        crate::scan::BundleContent::StateSnapshot => todo!(),
        crate::scan::BundleContent::Transaction => {
            let (txs, _) = extract_current_transactions(&man.archive_dir).await?;
            let batch_res = load_tx_cypher::tx_batch(&txs, pool, 1000, &man.archive_id).await?;
            all_results.increment(&batch_res);
        }
        crate::scan::BundleContent::EpochEnding => todo!(),
    }
    Ok(all_results)
}

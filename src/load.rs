use crate::{
    batch_tx_type::BatchTxReturn,
    extract_snapshot::{extract_current_snapshot, extract_v5_snapshot},
    extract_transactions::extract_current_transactions,
    load_account_state::snapshot_batch,
    load_tx_cypher,
    queue::{self, clear_queue, push_queue_from_archive_map},
    scan::{ArchiveMap, ManifestInfo},
    unzip_temp,
};

use anyhow::{bail, Context, Result};
use log::{error, info, warn};
use neo4rs::Graph;

/// takes all the archives from a map, and tries to load them sequentially
pub async fn ingest_all(
    archive_map: &ArchiveMap,
    pool: &Graph,
    force_queue: bool,
    batch_size: usize,
) -> Result<()> {
    // clear the queue and enqueue all these jobs
    if force_queue {
        warn!(
            "clearing load queue, and enqueueing all archives, count: {}",
            archive_map.0.len()
        );
        clear_queue(pool).await.context("could not clear queue")?;
        // NOTE: this does not infer batches. That is done at the actual
        // load controller level.
        push_queue_from_archive_map(archive_map, pool)
            .await
            .context("could not push queue")?;
    }

    // Lazy check to see what is remaining from previous run
    // don't bother extracting archives which we loaded successfully prior
    // Note that the inner tx_batch will also check if anything has already
    // been inserted perhaps concurrently to the start of this process.
    // get queue of any batch which has any incomplete batches
    let pending = queue::get_queued(pool).await?;
    info!("pending archives: {}", pending.len());

    // This manifest may be for a .gz file, we should handle here as well
    for (_p, m) in archive_map.0.iter() {
        println!(
            "\nProcessing: {:?} with archive: {}",
            m.contents,
            m.archive_dir.display()
        );

        let complete = queue::are_all_completed(pool, &m.archive_id).await?;

        if !complete {
            info!("checking if we need to decompress");
            let (new_unzip_path, temp) = unzip_temp::maybe_handle_gz(&m.archive_dir)?;
            let mut better_man = ManifestInfo::new(&new_unzip_path);
            better_man.set_info()?;

            let batch_tx_return = try_load_one_archive(&better_man, pool, batch_size).await?;
            println!("SUCCESS: {}", batch_tx_return);
            drop(temp);
        } else {
            info!(
                "archive complete (or not in queue): {}",
                m.archive_dir.display()
            );
        }
    }

    Ok(())
}

pub async fn try_load_one_archive(
    man: &ManifestInfo,
    pool: &Graph,
    batch_size: usize,
) -> Result<BatchTxReturn> {
    let mut all_results = BatchTxReturn::new();
    match man.contents {
        crate::scan::BundleContent::Unknown => todo!(),
        crate::scan::BundleContent::StateSnapshot => {
            let snaps = match man.version {
                crate::scan::FrameworkVersion::Unknown => {
                    error!("no framework version detected");
                    bail!("could not load archive from manifest");
                }
                crate::scan::FrameworkVersion::V5 => extract_v5_snapshot(&man.archive_dir).await?,
                crate::scan::FrameworkVersion::V6 => {
                    extract_current_snapshot(&man.archive_dir).await?
                }
                crate::scan::FrameworkVersion::V7 => {
                    extract_current_snapshot(&man.archive_dir).await?
                }
            };
            snapshot_batch(&snaps, pool, batch_size, &man.archive_id).await?;
        }
        crate::scan::BundleContent::Transaction => {
            let (txs, _) = extract_current_transactions(&man.archive_dir, &man.version).await?;
            let batch_res =
                load_tx_cypher::tx_batch(&txs, pool, batch_size, &man.archive_id).await?;
            all_results.increment(&batch_res);
        }
        crate::scan::BundleContent::EpochEnding => todo!(),
    }
    Ok(all_results)
}

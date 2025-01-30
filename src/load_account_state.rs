use std::{thread, time::Duration};

use crate::{batch_tx_type::BatchTxReturn, queue, schema_account_state::WarehouseAccState};
use anyhow::{Context, Result};
use log::{error, info, warn};
use neo4rs::Graph;

// TODO: code duplication
pub async fn snapshot_batch(
    txs: &[WarehouseAccState],
    pool: &Graph,
    batch_size: usize,
    archive_id: &str,
) -> Result<BatchTxReturn> {
    let mut all_results = BatchTxReturn::new();

    let chunks: Vec<&[WarehouseAccState]> = txs.chunks(batch_size).collect();

    info!("archive: {}", archive_id);

    for (i, c) in chunks.into_iter().enumerate() {
        info!("batch #{}", i);
        // double checking the status of the loading PER BATCH
        // it could have been updated in the interim
        // since the outer check in ingest_all, just checks
        // all things completed prior to this run
        // check if this is already completed, or should be inserted.
        match queue::is_batch_complete(pool, archive_id, i).await {
            Ok(Some(true)) => {
                info!("...skipping, all batches loaded.");
                // skip this one
                continue;
            }
            Ok(Some(false)) => {
                // keep going
            }
            _ => {
                info!("...batch not found in queue, adding to queue.");

                // no task found in db, add to queue
                queue::update_task(pool, archive_id, false, i).await?;
            }
        }
        info!("...loading to db");

        match impl_batch_snapshot_insert(pool, c).await {
            Ok(batch) => {
                all_results.increment(&batch);
                queue::update_task(pool, archive_id, true, i).await?;
                info!("...success");
            }
            Err(e) => {
                let secs = 10;
                error!("skipping batch, could not insert: {:?}", e);
                warn!("waiting {} secs before retrying connection", secs);
                thread::sleep(Duration::from_secs(secs));
            }
        };
    }

    Ok(all_results)
}

pub async fn impl_batch_snapshot_insert(
    pool: &Graph,
    batch_snapshots: &[WarehouseAccState],
) -> Result<BatchTxReturn> {
    let list_str = WarehouseAccState::to_cypher_map(batch_snapshots);
    let cypher_string = WarehouseAccState::cypher_batch_insert_str(&list_str);

    // Execute the query
    let cypher_query = neo4rs::query(&cypher_string);
    let mut res = pool
        .execute(cypher_query)
        .await
        .context("execute query error")?;

    let row = res.next().await?.context("no row returned")?;

    let merged_snapshots: u64 = row
        .get("merged_snapshots")
        .context("no unique_accounts field")?;

    info!("merged snapshots: {}", merged_snapshots);

    Ok(BatchTxReturn {
        unique_accounts: 0,
        created_accounts: 0,
        modified_accounts: 0,
        unchanged_accounts: 0,
        created_tx: merged_snapshots,
    })
}

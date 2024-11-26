use anyhow::{Context, Result};
use log::{error, info};
use neo4rs::{query, Graph};

use crate::{
    batch_tx_type::BatchTxReturn,
    cypher_templates::{write_batch_tx_string, write_batch_user_create},
    queue,
    schema_transaction::WarehouseTxMaster,
};

// TODO: code duplication with exchange order loading.
pub async fn tx_batch(
    txs: &[WarehouseTxMaster],
    pool: &Graph,
    batch_size: usize,
    archive_id: &str,
) -> Result<BatchTxReturn> {
    let chunks: Vec<&[WarehouseTxMaster]> = txs.chunks(batch_size).collect();
    let mut all_results = BatchTxReturn::new();
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

        match impl_batch_tx_insert(pool, c).await {
            Ok(batch) => {
                all_results.increment(&batch);
                queue::update_task(pool, archive_id, true, i).await?;
                info!("...success");
            }
            Err(e) => {
                error!("could not insert batch: {:?}", e);
                ////////
                // TODO: do we need to handle connection errors?
                // let secs = 10;
                // warn!("waiting {} secs before retrying connection", secs);
                // thread::sleep(Duration::from_secs(secs));
                ////////
            }
        };
    }

    Ok(all_results)
}

pub async fn impl_batch_tx_insert(
    pool: &Graph,
    batch_txs: &[WarehouseTxMaster],
) -> Result<BatchTxReturn> {
    let mut unique_addrs = vec![];
    batch_txs.iter().for_each(|t| {
        if !unique_addrs.contains(&t.sender) {
            unique_addrs.push(t.sender);
        }
        if let Some(r) = t.relation_label.get_recipient() {
            if !unique_addrs.contains(&r) {
                unique_addrs.push(r);
            }
        }
    });

    info!("unique accounts in batch: {}", unique_addrs.len());

    let list_str = WarehouseTxMaster::to_cypher_map(batch_txs);

    // first insert the users
    // cypher queries makes it annoying to do a single insert of users and
    // txs
    let cypher_string = write_batch_user_create(&list_str);
    // dbg!(format!("{:#}",cypher_string));

    // Execute the query
    let cypher_query = query(&cypher_string);
    let mut res = pool
        .execute(cypher_query)
        .await
        .context("execute query error")?;

    let row = res.next().await?.context("no row returned")?;

    let unique_accounts: u64 = row
        .get("unique_accounts")
        .context("no unique_accounts field")?;
    let created_accounts: u64 = row
        .get("created_accounts")
        .context("no created_accounts field")?;
    let modified_accounts: u64 = row
        .get("modified_accounts")
        .context("no modified_accounts field")?;
    let unchanged_accounts: u64 = row
        .get("unchanged_accounts")
        .context("no unchanged_accounts field")?;

    let cypher_string = write_batch_tx_string(&list_str);
    // Execute the query
    let cypher_query = query(&cypher_string);
    let mut res = pool.execute(cypher_query).await.context(format!(
        "execute query error. Query string: {:#}",
        &cypher_string
    ))?;
    let row = res.next().await?.context("no row returned")?;
    let created_tx: u64 = row.get("created_tx").context("no created_tx field")?;

    if unique_accounts != unique_addrs.len() as u64 {
        error!(
            "number of accounts in batch {} is not equal to unique accounts in query: {}",
            unique_addrs.len(),
            unique_accounts,
        );
    }

    Ok(BatchTxReturn {
        unique_accounts,
        created_accounts,
        modified_accounts,
        unchanged_accounts,
        created_tx,
    })
}

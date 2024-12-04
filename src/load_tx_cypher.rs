use anyhow::{Context, Result};
use log::{error, info};
use neo4rs::{query, Graph};

use crate::{
    batch_tx_type::BatchTxReturn,
    cypher_templates::{to_cypher_object, write_batch_tx_string, write_batch_user_create},
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
    info!("archive: {}", archive_id);

    if txs.is_empty() {
        // mark as complete so we don't retry
        queue::update_task(pool, archive_id, true, 0).await?;
    }

    let chunks: Vec<&[WarehouseTxMaster]> = txs.chunks(batch_size).collect();
    let mut all_results = BatchTxReturn::new();

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

pub fn alt_write_batch_tx_string(txs: &[WarehouseTxMaster]) -> Result<String> {
    let mut inserts = "".to_string();
    for t in txs {
        let mut maybe_args = "".to_string();
        if let Some(ef) = &t.entry_function {
            maybe_args = format!("SET rel += {},", to_cypher_object(ef)?);
        }

        let each_insert = format!(
            r#"
MERGE (from:Account {{address: '{sender}' }})
MERGE (to:Account {{address: '{recipient}' }})
MERGE (from)-[rel:{relation} {{
  tx_hash: '{tx_hash}',
  block_datetime: '{block_datetime}',
  block_timestamp: '{block_timestamp}',
  function: '{function}'
}}]->(to)

ON CREATE SET rel.created_at = timestamp(), rel.modified_at = null
{maybe_args}

ON MATCH SET rel.modified_at = timestamp()
{maybe_args}

"#,
            sender = t.sender.to_hex_literal(),
            recipient = t
                .relation_label
                .get_recipient()
                .unwrap_or(t.sender)
                .to_hex_literal(),
            tx_hash = t.tx_hash,
            relation = t.relation_label.to_cypher_label(),
            function = t.function,
            block_datetime = t.block_datetime.to_rfc3339(),
            block_timestamp = t.block_timestamp,
        );

        inserts = format!("{}\n{}", inserts, each_insert);
    }

    let query = format!(
        r#"
{}

WITH rel
RETURN
  COUNT(CASE WHEN rel.created_at = timestamp() THEN 1 END) AS created_tx,
  COUNT(CASE WHEN rel.modified_at = timestamp() AND rel.created_at < timestamp() THEN 1 END) AS modified_tx
"#,
        inserts
    );

    Ok(query)
}

#[test]

fn test_each_insert() {
    let tx1 = WarehouseTxMaster::default();
    let s = alt_write_batch_tx_string(&vec![tx1]);
    dbg!(&s);
}

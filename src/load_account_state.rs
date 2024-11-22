use log::info;
use neo4rs::Graph;
use anyhow::{Context, Result};
use crate::schema_account_state::WarehouseAccState;



pub async fn impl_batch_snapshot_insert(
    pool: &Graph,
    batch_snapshots: &[WarehouseAccState],
) -> Result<()> {

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

    Ok(())
}

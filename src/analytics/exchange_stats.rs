use std::{sync::Arc, thread::available_parallelism};

use anyhow::{Context, Result};
use log::{info, warn};
use neo4rs::Graph;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Debug, Serialize, Deserialize)]
pub struct RMSResults {
    id: u64,
    time: String,
    matching_trades: u64,
    rms: f64,
}

static BATCH_SIZE: u64 = 100;

pub async fn query_rms_analytics(pool: &Graph, threads: Option<usize>) -> Result<Vec<RMSResults>> {
    let threads = threads.unwrap_or(available_parallelism().unwrap().get());

    let n = query_trades_count(pool).await?;

    let mut batches = 1;
    if n > BATCH_SIZE {
        batches = (n / BATCH_SIZE) + 1
    };


    let semaphore = Arc::new(Semaphore::new(threads)); // Semaphore to limit concurrency
    let mut tasks = vec![];

    for batch_sequence in 0..batches {
        let pool = pool.clone(); // Clone pool for each task
        let semaphore = Arc::clone(&semaphore); // Clone semaphore for each task

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await; // Acquire semaphore permit
            info!("PROGRESS: {batch_sequence}/{n}");
            query_rms_analytics_chunk(&pool, batches).await // Perform the task
        });

        tasks.push(task);
    }

    // // Await all tasks and handle results
    let results = futures::future::join_all(tasks).await;

    let mut rms_vec = vec![];
    for el in results {
        let mut v = el??;
        rms_vec.append(&mut v);
    }
    Ok(rms_vec)
}

// get rms analytics on transaction
pub async fn query_rms_analytics_chunk(
    pool: &Graph,
    batch_sequence: u64,
) -> Result<Vec<RMSResults>> {
    let cypher_string = format!(
        r#"
MATCH (from_user:SwapAccount)-[t:Swap]->(to_accepter:SwapAccount)
ORDER BY t.filled_at
SKIP 100 * {batch_sequence} LIMIT 100
WITH DISTINCT t as txs, from_user, to_accepter, t.filled_at AS current_time

MATCH (from_user2:SwapAccount)-[other:Swap]->(to_accepter2:SwapAccount)
WHERE datetime(other.filled_at) >= datetime(current_time) - duration({{ hours: 1 }})
  AND datetime(other.filled_at) < datetime(current_time)
  AND (from_user2 <> from_user OR from_user2 <> to_accepter OR to_accepter2 <> from_user OR to_accepter2 <> to_accepter)  // Exclude same from_user and to_accepter
// WITH txs, other, sqrt(avg(other.price * other.price)) AS rms
RETURN id(txs) AS id, txs.filled_at AS time, COUNT(other) AS matching_trades, sqrt(avg(other.price * other.price)) AS rms
      "#
    );
    let cypher_query = neo4rs::query(&cypher_string);

    let mut res = pool
        .execute(cypher_query)
        .await
        .context("execute query error")?;

    let mut results = vec![];
    while let Some(row) = res.next().await? {
        match row.to::<RMSResults>() {
            Ok(r) => results.push(r),
            Err(e) => {
                warn!("unknown row returned {}", e)
            }
        }
    }

    Ok(results)
}

// get rms analytics on transaction
pub async fn query_trades_count(pool: &Graph) -> Result<u64> {
    let cypher_string = r#"
MATCH (:SwapAccount)-[t:Swap]->(:SwapAccount)
RETURN COUNT(DISTINCT t) as trades_count
      "#
    .to_string();
    let cypher_query = neo4rs::query(&cypher_string);

    let mut res = pool
        .execute(cypher_query)
        .await
        .context("execute query error")?;

    while let Some(row) = res.next().await? {
        match row.get::<i64>("trades_count") {
            Ok(r) => return Ok(r as u64),
            Err(e) => {
                warn!("unknown row returned {}", e);
            }
        }
    }

    anyhow::bail!("no trades_count found");
}

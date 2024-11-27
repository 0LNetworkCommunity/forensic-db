use crate::{
    json_rescue_v5_extract::{
        decompress_to_temppath, extract_v5_json_rescue, list_all_json_files, list_all_tgz_archives,
    },
    load_tx_cypher::tx_batch,
    schema_transaction::WarehouseTxMaster,
};
use anyhow::Result;
use futures::{stream, StreamExt};
use log::{error, info};
use neo4rs::Graph;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task;

/// How many records to read from the archives before attempting insert
static LOAD_QUEUE_SIZE: usize = 1000;
/// When we attempt insert, the chunks of txs that go in to each query
static QUERY_BATCH_SIZE: usize = 250;

/// from a tgz file decompress all the .json files in archive
/// and then read into the warehouse record format
pub async fn decompress_and_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    let mut found_count = 0u64;
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
            let res = tx_batch(
                &drain,
                pool,
                QUERY_BATCH_SIZE,
                j.file_name().unwrap().to_str().unwrap(),
            )
            .await?;
            created_count += res.created_tx as u64;
            found_count += drain.len() as u64;
        }
    }

    info!("V5 transactions found: {}", found_count);
    info!("V5 transactions processed: {}", created_count);
    if found_count != created_count {
        error!("transactions loaded don't match transactions extracted");
    }
    Ok(created_count)
}

const MAX_CONCURRENT_PARSE: usize = 4; // Number of concurrent parsing tasks
const MAX_CONCURRENT_INSERT: usize = 2; // Number of concurrent database insert tasks

pub async fn concurrent_decompress_and_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    let found_count = Arc::new(tokio::sync::Mutex::new(0u64));
    let created_count = Arc::new(tokio::sync::Mutex::new(0u64));

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_INSERT));
    let parse_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PARSE));

    let tasks = json_vec.into_iter().map(|j| {
        let semaphore = Arc::clone(&semaphore);
        let parse_semaphore = Arc::clone(&parse_semaphore);
        let found_count = Arc::clone(&found_count);
        let created_count = Arc::clone(&created_count);
        let pool = pool.clone();

        task::spawn(async move {
            let _permit = parse_semaphore.acquire().await.unwrap(); // Control parsing concurrency
            if let Ok((mut r, _e)) = extract_v5_json_rescue(&j) {
                let drain: Vec<WarehouseTxMaster> = r.drain(..).collect();

                if !drain.is_empty() {
                    let _db_permit = semaphore.acquire().await.unwrap(); // Control DB insert concurrency
                    let res = tx_batch(
                        &drain,
                        &pool,
                        QUERY_BATCH_SIZE,
                        j.file_name().unwrap().to_str().unwrap(),
                    )
                    .await?;
                    {
                        let mut fc = found_count.lock().await;
                        let mut cc = created_count.lock().await;
                        *fc += drain.len() as u64;
                        *cc += res.created_tx as u64;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        })
    });

    // Collect all results
    let results: Vec<_> = futures::future::join_all(tasks).await;

    // Check for errors in tasks
    for result in results {
        if let Err(e) = result {
            error!("Task failed: {:?}", e);
        }
    }

    let found_count = *found_count.lock().await;
    let created_count = *created_count.lock().await;

    info!("V5 transactions found: {}", found_count);
    info!("V5 transactions processed: {}", created_count);
    if found_count != created_count {
        error!("transactions loaded don't match transactions extracted");
    }

    Ok(created_count)
}

use futures::{stream, StreamExt};
use tokio::sync::Semaphore;
use std::sync::Arc;

const MAX_CONCURRENT_PARSE: usize = 4; // Number of concurrent parsing tasks
const MAX_CONCURRENT_INSERT: usize = 2; // Number of concurrent database insert tasks

pub async fn stream_decompress_and_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    let found_count = Arc::new(tokio::sync::Mutex::new(0u64));
    let created_count = Arc::new(tokio::sync::Mutex::new(0u64));

    let parse_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PARSE));
    let insert_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_INSERT));

    // Stream for parsing JSON files
    let parse_stream = stream::iter(json_vec).map(|j| {
        let parse_semaphore = Arc::clone(&parse_semaphore);
        let insert_semaphore = Arc::clone(&insert_semaphore);
        let found_count = Arc::clone(&found_count);
        let created_count = Arc::clone(&created_count);
        let pool = pool.clone();

        async move {
            let _parse_permit = parse_semaphore.acquire().await.unwrap();
            if let Ok((mut records, _e)) = extract_v5_json_rescue(&j) {
                let batch = records.drain(..).collect::<Vec<_>>();

                if !batch.is_empty() {
                    let _insert_permit = insert_semaphore.acquire().await.unwrap();
                    let res = tx_batch(
                        &batch,
                        &pool,
                        QUERY_BATCH_SIZE,
                        j.file_name().unwrap().to_str().unwrap(),
                    )
                    .await?;

                    let mut fc = found_count.lock().await;
                    let mut cc = created_count.lock().await;
                    *fc += batch.len() as u64;
                    *cc += res.created_tx as u64;
                }
            }
            Ok::<(), anyhow::Error>(())
        }
    });

    // Process the stream with controlled concurrency
    parse_stream
        .buffer_unordered(MAX_CONCURRENT_PARSE)
        .for_each(|result| async {
            if let Err(e) = result {
                error!("Failed to process file: {:?}", e);
            }
        })
        .await;

    // Gather final counts
    let found_count = *found_count.lock().await;
    let created_count = *created_count.lock().await;

    info!("V5 transactions found: {}", found_count);
    info!("V5 transactions processed: {}", created_count);
    if found_count != created_count {
        error!("transactions loaded don't match transactions extracted");
    }

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

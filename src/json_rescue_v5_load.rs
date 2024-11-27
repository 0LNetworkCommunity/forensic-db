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
use tokio::sync::{Mutex, Semaphore};
use tokio::task;

/// How many records to read from the archives before attempting insert
// static LOAD_QUEUE_SIZE: usize = 1000;
/// When we attempt insert, the chunks of txs that go in to each query
static QUERY_BATCH_SIZE: usize = 250;

/// from a tgz file decompress all the .json files in archive
/// and then read into the warehouse record format
pub async fn single_thread_decompress_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    let mut found_count = 0u64;
    let mut created_count = 0u64;

    let mut unique_functions: Vec<String> = vec![];

    for j in json_vec {
        let (records, _, unique) = extract_v5_json_rescue(&j)?;

        unique.iter().for_each(|f| {
            if !unique_functions.contains(&f) {
                unique_functions.push(f.clone());
            }
        });

        let res = tx_batch(
            &records,
            pool,
            QUERY_BATCH_SIZE,
            j.file_name().unwrap().to_str().unwrap(),
        )
        .await?;
        created_count += res.created_tx as u64;
        found_count += records.len() as u64;
    }

    info!("V5 transactions found: {}", found_count);
    info!("V5 transactions processed: {}", created_count);
    if found_count != created_count {
        error!("transactions loaded don't match transactions extracted");
    }
    Ok(created_count)
}

const MAX_CONCURRENT_PARSE: usize = 24; // Number of concurrent parsing tasks
const MAX_CONCURRENT_INSERT: usize = 4; // Number of concurrent database insert tasks

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
            if let Ok((mut r, _e, _)) = extract_v5_json_rescue(&j) {
                let drain: Vec<WarehouseTxMaster> = std::mem::take(&mut r);

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

pub async fn stream_decompress_and_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    let found_count = Arc::new(Mutex::new(0u64));
    let created_count = Arc::new(Mutex::new(0u64));

    let v: Vec<String> = vec![];
    let unique_functions = Arc::new(Mutex::new(v));

    let parse_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PARSE));
    let insert_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_INSERT));

    // Stream for JSON file processing
    let _results: Vec<_> = stream::iter(json_vec)
        .map(|j| {
            let parse_semaphore = Arc::clone(&parse_semaphore);
            let insert_semaphore = Arc::clone(&insert_semaphore);
            let found_count = Arc::clone(&found_count);
            let created_count = Arc::clone(&created_count);
            let unique_functions = Arc::clone(&unique_functions);

            let pool = pool.clone();

            async move {
                let _parse_permit = parse_semaphore.acquire().await.unwrap();

                if let Ok((records, _e, unique_fun)) = extract_v5_json_rescue(&j) {
                    // let batch = std::mem::take(&mut records);

                    if !records.is_empty() {
                        let _insert_permit = insert_semaphore.acquire().await.unwrap();
                        let res = tx_batch(
                            &records,
                            &pool,
                            QUERY_BATCH_SIZE,
                            j.file_name().unwrap().to_str().unwrap(),
                        )
                        .await?;

                        let mut uf = unique_functions.lock().await;
                        for f in &unique_fun {
                            if !uf.contains(&f) {
                                uf.push(f.to_owned());
                            }
                        }

                        let mut fc = found_count.lock().await;
                        let mut cc = created_count.lock().await;

                        *fc += records.len() as u64;
                        *cc += res.created_tx;
                    }
                }

                Ok::<(), anyhow::Error>(())
            }
        })
        .buffer_unordered(MAX_CONCURRENT_PARSE) // Concurrency for parsing
        .collect() // Waits for all tasks to finish
        .await;

    // // Check for errors in results
    // for result in results {
    //     if let Err(e) = result {
    //         error!("Task failed: {:?}", e);
    //     }
    // }

    // Gather final counts
    let found_count = *found_count.lock().await;
    let created_count = *created_count.lock().await;
    let unique_functions = unique_functions.lock().await;

    info!("unique functions found: {:?}", unique_functions);

    info!("V5 transactions found: {}", found_count);
    info!("V5 transactions processed: {}", created_count);
    if found_count != created_count {
        error!("transactions loaded don't match transactions extracted");
    }

    Ok(created_count)
}

pub async fn alt_stream_decompress_and_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    let json_vec = list_all_json_files(temppath.path())?;

    let found_count = Arc::new(tokio::sync::Mutex::new(0u64));
    let created_count = Arc::new(tokio::sync::Mutex::new(0u64));

    let parse_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PARSE));
    let insert_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_INSERT));

    // Parsing Stream
    let parsed_stream = stream::iter(json_vec).map({
        let parse_semaphore = Arc::clone(&parse_semaphore);

        move |j| {
            let parse_semaphore = Arc::clone(&parse_semaphore);
            async move {
                let _parse_permit = parse_semaphore.acquire().await;
                extract_v5_json_rescue(&j)
                    .map(|(records, _, _)| records)
                    .ok()
            }
        }
    });

    // Insertion Stream
    let results = parsed_stream
        .buffer_unordered(MAX_CONCURRENT_PARSE)
        .filter_map(|opt_records| async move { opt_records })
        .flat_map(|records| stream::iter(records.into_iter().collect::<Vec<_>>()))
        .chunks(QUERY_BATCH_SIZE) // Batch insert chunks
        .map({
            let insert_semaphore = Arc::clone(&insert_semaphore);
            let found_count = Arc::clone(&found_count);
            let created_count = Arc::clone(&created_count);
            let pool = pool.clone();

            move |batch| {
                let insert_semaphore = Arc::clone(&insert_semaphore);
                let found_count = Arc::clone(&found_count);
                let created_count = Arc::clone(&created_count);
                let pool = pool.clone();

                async move {
                    let _insert_permit = insert_semaphore.acquire().await;
                    let res = tx_batch(&batch, &pool, QUERY_BATCH_SIZE, "batch").await?;
                    *found_count.lock().await += batch.len() as u64;
                    *created_count.lock().await += res.created_tx as u64;
                    Ok::<(), anyhow::Error>(())
                }
            }
        })
        .buffer_unordered(MAX_CONCURRENT_INSERT)
        .collect::<Vec<_>>() // Collect all results
        .await;

    // Log Errors
    for result in results {
        if let Err(e) = result {
            error!("Failed batch insert: {:?}", e);
        }
    }

    // Final Logging
    let found = found_count.lock().await;
    let created = created_count.lock().await;
    info!("V5 transactions found: {}", found);
    info!("V5 transactions processed: {}", created);
    if *found != *created {
        error!(
            "Mismatch: transactions loaded ({}) vs extracted ({})",
            created, found
        );
    }

    Ok(*created)
}

pub async fn rip(start_dir: &Path, pool: &Graph) -> Result<u64> {
    let tgz_list = list_all_tgz_archives(start_dir)?;
    info!("tgz archives found: {}", tgz_list.len());
    let mut txs = 0u64;
    for p in tgz_list.iter() {
        match alt_stream_decompress_and_extract(p, pool).await {
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

pub async fn rip_concurrent(start_dir: &Path, pool: &Graph) -> Result<()> {
    let tgz_list = list_all_tgz_archives(start_dir)?;
    info!("tgz archives found: {}", &tgz_list.len());

    let tasks: Vec<_> = tgz_list
        .into_iter()
        .map(|p| {
            let pool = pool.clone(); // Clone pool for each task
            tokio::spawn(async move {
                single_thread_decompress_extract(&p, &pool).await // Call the async function
            })
        })
        .collect();

    // Await all tasks and handle results
    let results = futures::future::join_all(tasks).await;
    // Check for errors
    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(_)) => {
                info!("Task {} completed successfully.", i);
            }
            Ok(Err(e)) => {
                error!("Task {} failed: {:?}", i, e);
            }
            Err(e) => {
                error!("Task {} panicked: {:?}", i, e);
            }
        }
    }
    Ok(())
}


const MAX_CONCURRENT_TASKS: usize = 4; // Define the limit for concurrent tasks

pub async fn rip_concurrent_limited(start_dir: &Path, pool: &Graph) -> Result<()> {
    let tgz_list = list_all_tgz_archives(start_dir)?;
    info!("tgz archives found: {}", tgz_list.len());

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TASKS)); // Semaphore to limit concurrency
    let mut tasks = vec![];

    for p in tgz_list.into_iter() {
        let pool = pool.clone(); // Clone pool for each task
        let semaphore = Arc::clone(&semaphore); // Clone semaphore for each task

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await; // Acquire semaphore permit
            single_thread_decompress_extract(&p, &pool).await // Perform the task
        });

        tasks.push(task);
    }

    // Await all tasks and handle results
    let results = futures::future::join_all(tasks).await;

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(_)) => {
                info!("Task {} completed successfully.", i);
            }
            Ok(Err(e)) => {
                error!("Task {} failed: {:?}", i, e);
            }
            Err(e) => {
                error!("Task {} panicked: {:?}", i, e);
            }
        }
    }

    Ok(())
}

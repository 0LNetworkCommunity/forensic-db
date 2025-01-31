use crate::{
    json_rescue_v5_extract::{
        decompress_to_temppath, extract_v5_json_rescue, list_all_json_files, list_all_tgz_archives,
    },
    load_tx_cypher::tx_batch,
    queue::{self},
};
use anyhow::Result;
use log::{error, info, trace, warn};
use neo4rs::Graph;
use std::sync::Arc;
use std::{path::Path, thread::available_parallelism};
use tokio::sync::Semaphore;

/// How many records to read from the archives before attempting insert
// static LOAD_QUEUE_SIZE: usize = 1000;
/// When we attempt insert, the chunks of txs that go in to each query
static QUERY_BATCH_SIZE: usize = 250;

/// from a tgz file decompress all the .json files in archive
/// and then read into the warehouse record format
pub async fn single_thread_decompress_extract(tgz_file: &Path, pool: &Graph) -> Result<u64> {
    let temppath = decompress_to_temppath(tgz_file)?;
    // for caching the archive
    let tgz_filename = tgz_file
        .file_name()
        .expect("could not find .tgz filename")
        .to_str()
        .unwrap();

    let json_vec = list_all_json_files(temppath.path())?;

    let mut found_count = 0u64;
    let mut created_count = 0u64;

    let mut unique_functions: Vec<String> = vec![];

    // TODO: the queue checks could be async, since many files are read
    for j in json_vec {
        let archive_id = j.file_name().unwrap().to_str().unwrap();
        // checks for .json cases remaining where we were interrupted mid .tgz archive.
        let complete = queue::are_all_completed(pool, archive_id).await?;
        if complete {
            trace!(
                "skip parsing {}, this file was loaded successfully",
                archive_id
            );
            continue;
        }

        let (records, _, unique) = extract_v5_json_rescue(&j)?;

        unique.iter().for_each(|f| {
            if !unique_functions.contains(f) {
                unique_functions.push(f.clone());
            }
        });

        let res = tx_batch(&records, pool, QUERY_BATCH_SIZE, archive_id).await?;
        created_count += res.created_tx as u64;
        found_count += records.len() as u64;
        queue::update_task(pool, tgz_filename, true, 0).await?;
    }
    if found_count > 0 && created_count > 0 {
        info!("V5 transactions found: {}", found_count);
        info!("V5 transactions inserted: {}", created_count);
        if found_count != created_count {
            warn!("transactions loaded don't match transactions extracted, perhaps previously loaded?");
        }
    } else {
        info!(
            "No transactions submitted, archive likely already loaded. File: {}",
            tgz_filename
        );
    }

    Ok(created_count)
}

pub async fn rip_concurrent_limited(
    start_dir: &Path,
    pool: &Graph,
    threads: Option<usize>,
) -> Result<u64> {
    let threads = threads.unwrap_or(available_parallelism().unwrap().get());
    info!("concurrent threads used: {}", threads);

    let tgz_list = list_all_tgz_archives(start_dir)?;
    let archives_count = tgz_list.len();
    info!("tgz archives found: {}", archives_count);

    let semaphore = Arc::new(Semaphore::new(threads)); // Semaphore to limit concurrency
    let mut tasks = vec![];

    for (n, tgz_path) in tgz_list.into_iter().enumerate() {
        let pool = pool.clone(); // Clone pool for each task

        let tgz_filename = tgz_path
            .file_name()
            .expect("could not find .tgz filename")
            .to_str()
            .unwrap();
        if queue::are_all_completed(&pool, tgz_filename).await? {
            info!("skipping, archive already loaded: {}", tgz_filename);
            continue;
        }

        let semaphore = Arc::clone(&semaphore); // Clone semaphore for each task

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await; // Acquire semaphore permit
            info!("PROGRESS: {n}/{archives_count}");

            single_thread_decompress_extract(&tgz_path, &pool).await // Perform the task
        });

        tasks.push(task);
    }

    // Await all tasks and handle results
    let results = futures::future::join_all(tasks).await;

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(n)) => {
                info!("Task {} completed successfully.", i);
                return Ok(n);
            }
            Ok(Err(e)) => {
                error!("Task {} failed: {:?}", i, e);
            }
            Err(e) => {
                error!("Task {} panicked: {:?}", i, e);
            }
        }
    }

    Ok(0)
}

mod support;
use anyhow::Result;
use diem_crypto::HashValue;

use libra_forensic_db::{
    cypher_templates::{alt_write_batch_tx_string, write_batch_user_create},
    extract_transactions::extract_current_transactions,
    load::try_load_one_archive,
    load_tx_cypher::tx_batch,
    neo4j_init::{get_neo4j_localhost_pool, maybe_create_indexes},
    scan::scan_dir_archive,
    schema_transaction::WarehouseTxMaster,
};
use neo4rs::query;
use support::neo4j_testcontainer::start_neo4j_container;

#[tokio::test]
async fn test_tx_batch() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();
    let archive_path = support::fixtures::v6_tx_manifest_fixtures_path();
    let (txs, _events) = extract_current_transactions(&archive_path).await?;
    assert!(txs.len() == 27);

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph)
        .await
        .expect("could start index");

    // load in batches
    let archive_id = archive_path.file_name().unwrap().to_str().unwrap();
    let res = tx_batch(&txs, &graph, 100, archive_id).await?;

    assert!(res.unique_accounts == 25);
    assert!(res.created_accounts == 25);
    assert!(res.modified_accounts == 0);
    assert!(res.unchanged_accounts == 0);
    assert!(res.created_tx == txs.len() as u64);

    let cypher_query = query(
        "MATCH ()-[r:Tx]->()
        RETURN count(r) AS total_tx_count",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;

    // Fetch the first row only
    let row = result.next().await?.unwrap();
    let total_tx_count: i64 = row.get("total_tx_count").unwrap();
    assert!(total_tx_count == txs.len() as i64);

    // check there are transaction records with function args.
    let cypher_query = query(
        "MATCH ()-[r:Tx]->()
        WHERE r.V7_OlAccountTransfer_amount IS NOT NULL
        RETURN COUNT(r) AS total_tx_count",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;

    // Fetch the first row only
    let row = result.next().await?.unwrap();
    let total_tx_count: i64 = row.get("total_tx_count").unwrap();

    assert!(total_tx_count == 24);

    Ok(())
}

#[tokio::test]
async fn test_load_entry_point_tx() -> anyhow::Result<()> {
    let archive_path = support::fixtures::v6_tx_manifest_fixtures_path();
    let archive = scan_dir_archive(&archive_path, None)?;
    let (_, man) = archive.0.first_key_value().unwrap();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph)
        .await
        .expect("could start index");

    let res = try_load_one_archive(man, &graph, 10).await?;

    assert!(res.unique_accounts == 31);
    assert!(res.created_accounts == 25);
    assert!(res.modified_accounts == 6);
    assert!(res.unchanged_accounts == 0);
    assert!(res.created_tx == 27);

    Ok(())
}

#[tokio::test]
async fn insert_with_cypher_string() -> Result<()> {
    let tx1 = WarehouseTxMaster {
        tx_hash: HashValue::random(),
        ..Default::default()
    };

    let tx2 = WarehouseTxMaster {
        tx_hash: HashValue::random(),
        ..Default::default()
    };

    let tx3 = WarehouseTxMaster {
        tx_hash: HashValue::random(),
        ..Default::default()
    };

    // two tx records
    let list = vec![tx1, tx2, tx3];

    let list_str = WarehouseTxMaster::to_cypher_map(&list);

    let cypher_string = alt_write_batch_tx_string(&list_str);

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph).await?;

    // Execute the query
    let cypher_query = query(&cypher_string);
    let mut res = graph.execute(cypher_query).await?;

    let row = res.next().await?.unwrap();
    // let created_accounts: i64 = row.get("created_accounts").unwrap();
    // dbg!(&created_accounts);
    // assert!(created_accounts == 1);
    // let modified_accounts: i64 = row.get("modified_accounts").unwrap();
    // assert!(modified_accounts == 0);
    // let unchanged_accounts: i64 = row.get("unchanged_accounts").unwrap();
    // assert!(unchanged_accounts == 0);
    let created_tx: i64 = row.get("created_tx").unwrap();
    assert!(created_tx == 3);

    // get the sum of all transactions in db
    let cypher_query = query(
        "MATCH ()-[r:Tx]->()
         RETURN count(r) AS total_tx_count",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;
    let row = result.next().await?.unwrap();
    let total_tx_count: i64 = row.get("total_tx_count").unwrap();
    assert!(total_tx_count == 3);
    Ok(())
}

#[tokio::test]
async fn batch_users_create_unit() -> Result<()> {
    let tx1 = WarehouseTxMaster {
        tx_hash: HashValue::random(),
        ..Default::default()
    };

    let tx2 = WarehouseTxMaster {
        tx_hash: HashValue::random(),
        ..Default::default()
    };

    let tx3 = WarehouseTxMaster {
        tx_hash: HashValue::random(),
        ..Default::default()
    };

    // two tx records
    let list = vec![tx1, tx2, tx3];

    let list_str = WarehouseTxMaster::to_cypher_map(&list);

    let cypher_string = write_batch_user_create(&list_str);

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph).await?;

    // Execute the query
    let cypher_query = query(&cypher_string);
    let mut res = graph.execute(cypher_query).await?;

    let row = res.next().await?.unwrap();
    let created_accounts: i64 = row.get("created_accounts").unwrap();

    assert!(created_accounts == 1);
    let modified_accounts: i64 = row.get("modified_accounts").unwrap();
    assert!(modified_accounts == 0);
    let unchanged_accounts: i64 = row.get("unchanged_accounts").unwrap();
    assert!(unchanged_accounts == 0);

    // get the sum of all transactions in db
    let cypher_query = query(
        "MATCH (a:Account)
         RETURN count(a) AS total_users",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;
    let row = result.next().await?.unwrap();
    let total_tx_count: i64 = row.get("total_users").unwrap();
    assert!(total_tx_count == 1);

    Ok(())
}

// NOTE: Left commented for reference. Bolt types deprecated in favor of string templates
// #[ignore]
// #[tokio::test]
// async fn test_bolt_serialize() -> Result<()> {
//     let c = start_neo4j_container();
//     let port = c.get_host_port_ipv4(7687);
//     let graph = get_neo4j_localhost_pool(port)
//         .await
//         .expect("could not get neo4j connection pool");
//     maybe_create_indexes(&graph).await?;

//     // Define a batch of transactions as a vector of HashMaps
//     let transactions = vec![WarehouseTxMaster::default()];
//     let bolt_list = WarehouseTxMaster::slice_to_bolt_list(&transactions);

//     // Build the query and add the transactions as a parameter
//     let cypher_query = query(
//         "UNWIND $transactions AS tx
//          MERGE (from:Account {address: tx.sender})
//          MERGE (to:Account {address: tx.recipient})
//          MERGE (from)-[:Tx {tx_hash: tx.tx_hash}]->(to)",
//     )
//     .param("transactions", bolt_list); // Pass the batch as a parameter

//     // Execute the query
//     graph.run(cypher_query).await?;

//     // get the sum of all transactions in db
//     let cypher_query = query(
//         "MATCH ()-[r:Tx]->()
//          RETURN count(r) AS total_tx_count",
//     );

//     // Execute the query
//     let mut result = graph.execute(cypher_query).await?;

//     // Fetch the first row only
//     let row = result.next().await?.unwrap();
//     let total_tx_count: i64 = row.get("total_tx_count").unwrap();
//     assert!(total_tx_count == 1);

//     Ok(())
// }

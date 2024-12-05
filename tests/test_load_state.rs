mod support;

use libra_forensic_db::{
    extract_snapshot::{extract_current_snapshot, extract_v5_snapshot},
    load_account_state::{impl_batch_snapshot_insert, snapshot_batch},
    neo4j_init::{get_neo4j_localhost_pool, maybe_create_indexes},
    schema_account_state::WarehouseAccState,
};
use support::{
    fixtures::{v5_state_manifest_fixtures_path, v7_state_manifest_fixtures_path},
    neo4j_testcontainer::start_neo4j_container,
};

#[tokio::test]
async fn test_snapshot_unit() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();

    let snap1 = WarehouseAccState::default();
    let snap2 = WarehouseAccState::default();
    let snap3 = WarehouseAccState::default();
    let vec_snap = vec![snap1, snap2, snap3];

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph)
        .await
        .expect("could start index");

    let merged_snapshots = impl_batch_snapshot_insert(&graph, &vec_snap).await?;
    assert!(merged_snapshots.created_tx == 3);

    Ok(())
}

#[tokio::test]
async fn test_snapshot_batch() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();
    let archive_path = v7_state_manifest_fixtures_path();
    assert!(archive_path.exists());
    let vec_snap = extract_current_snapshot(&archive_path).await?;

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph)
        .await
        .expect("could start index");

    let merged_snapshots = impl_batch_snapshot_insert(&graph, &vec_snap[..100]).await?;

    assert!(merged_snapshots.created_tx == 100);

    // check DB to see what is persisted
    let cypher_query = neo4rs::query(
        "MATCH ()-[r:State]->()
         RETURN count(r) AS count_state_edges",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;

    // Fetch the first row only
    let row = result.next().await?.unwrap();
    let count: i64 = row.get("count_state_edges").unwrap();

    assert!(count == 100i64);

    Ok(())
}

#[tokio::test]
async fn test_v5_snapshot_batch() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();
    let manifest_file = v5_state_manifest_fixtures_path().join("state.manifest");
    assert!(manifest_file.exists());
    let vec_snap = extract_v5_snapshot(&manifest_file).await?;

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph)
        .await
        .expect("could start index");

    let merged_snapshots = impl_batch_snapshot_insert(&graph, &vec_snap[..100]).await?;
    assert!(merged_snapshots.created_tx == 100);

    // check DB to see what is persisted
    let cypher_query = neo4rs::query(
        "MATCH ()-[r:State]->()
         RETURN count(r) AS count_state_edges",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;

    // Fetch the first row only
    let row = result.next().await?.unwrap();
    let count: i64 = row.get("count_state_edges").unwrap();
    assert!(count == 100i64);

    Ok(())
}

#[tokio::test]
async fn test_snapshot_entrypoint() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();
    let manifest_file = v5_state_manifest_fixtures_path().join("state.manifest");
    assert!(manifest_file.exists());
    let vec_snap = extract_v5_snapshot(&manifest_file).await?;
    assert!(vec_snap.len() == 17338);

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph)
        .await
        .expect("could start index");

    let merged_snapshots = snapshot_batch(&vec_snap, &graph, 1000, "test_v5_manifest").await?;

    assert!(merged_snapshots.created_tx == 17338);

    // check DB to see what is persisted
    let cypher_query = neo4rs::query(
        "MATCH ()-[r:State]->()
         RETURN count(r) AS count_state_edges",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;

    // Fetch the first row only
    let row = result.next().await?.unwrap();
    let count: i64 = row.get("count_state_edges").unwrap();
    dbg!(&count);
    assert!(count == 17338i64);

    Ok(())
}


mod support;

use libra_forensic_db::{
    extract_snapshot::extract_v5_snapshot, load_account_state::impl_batch_snapshot_insert, neo4j_init::{get_neo4j_localhost_pool, maybe_create_indexes}
};
use support::{fixtures::v5_state_manifest_fixtures_path, neo4j_testcontainer::start_neo4j_container};

#[tokio::test]
async fn test_snapshot_batch() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();
    let manifest_file = v5_state_manifest_fixtures_path().join("state.manifest");
    assert!(manifest_file.exists());
    let s = extract_v5_snapshot(&manifest_file).await?;


    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&graph)
        .await
        .expect("could start index");

    impl_batch_snapshot_insert(&graph, &s).await?;


    Ok(())
}

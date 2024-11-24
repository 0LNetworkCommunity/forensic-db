mod support;

use libra_forensic_db::{
    json_rescue_v5_load,
    neo4j_init::{get_neo4j_localhost_pool, maybe_create_indexes},
};
use support::{fixtures, neo4j_testcontainer::start_neo4j_container};

#[tokio::test]
async fn test_load_all_tgz() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let pool = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&pool)
        .await
        .expect("could start index");

    let path = fixtures::v5_json_tx_path().join("0-99900.tgz");

    let tx_count = json_rescue_v5_load::e2e_decompress_and_extract(&path, &pool).await?;
    dbg!(&tx_count);
    assert!(tx_count == 6157);

    Ok(())
}

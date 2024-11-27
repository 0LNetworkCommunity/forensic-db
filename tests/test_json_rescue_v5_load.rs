mod support;

use libra_forensic_db::{
    json_rescue_v5_extract::extract_v5_json_rescue,
    json_rescue_v5_load,
    load_tx_cypher::tx_batch,
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

    let tx_count = json_rescue_v5_load::decompress_and_extract(&path, &pool).await?;

    assert!(tx_count == 5244);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_load_all_tgz() -> anyhow::Result<()> {
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

    let tx_count = json_rescue_v5_load::concurrent_decompress_and_extract(&path, &pool).await?;

    assert!(tx_count == 5244);

    Ok(())
}

#[tokio::test]
async fn test_stream_load_all_tgz() -> anyhow::Result<()> {
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

    let tx_count = json_rescue_v5_load::stream_decompress_and_extract(&path, &pool).await?;

    assert!(tx_count == 13);

    Ok(())
}

#[tokio::test]
async fn test_load_entrypoint() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let pool = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&pool)
        .await
        .expect("could start index");

    let path = fixtures::v5_json_tx_path();

    let tx_count = json_rescue_v5_load::rip(&path, &pool).await?;
    dbg!(&tx_count);
    assert!(tx_count == 13);

    Ok(())
}

#[tokio::test]
async fn test_rescue_v5_parse_set_wallet_tx() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();

    let path = fixtures::v5_json_tx_path().join("example_set_wallet_type.json");

    let (vec_tx, _) = extract_v5_json_rescue(&path)?;

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let pool = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&pool)
        .await
        .expect("could start index");

    let _res = tx_batch(&vec_tx, &pool, 100, "test-set-wallet").await?;

    Ok(())
}

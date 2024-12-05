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

    let tx_count = json_rescue_v5_load::single_thread_decompress_extract(&path, &pool).await?;

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

    let tx_count = json_rescue_v5_load::rip_concurrent_limited(&path, &pool, None).await?;
    assert!(tx_count == 13);

    Ok(())
}

#[tokio::test]
async fn test_load_queue() -> anyhow::Result<()> {
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

    let tx_count = json_rescue_v5_load::rip_concurrent_limited(&path, &pool, None).await?;
    assert!(tx_count == 13);

    let tx_count = json_rescue_v5_load::rip_concurrent_limited(&path, &pool, None).await?;
    assert!(tx_count == 0);

    Ok(())
}

#[ignore]
// TODO: not a good test since we skip config tests in default mode
#[tokio::test]
async fn test_rescue_v5_parse_set_wallet_tx() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();

    let path = fixtures::v5_json_tx_path().join("example_set_wallet_type.json");

    let (vec_tx, _, _) = extract_v5_json_rescue(&path)?;
    dbg!(&vec_tx);

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let pool = get_neo4j_localhost_pool(port)
        .await
        .expect("could not get neo4j connection pool");
    maybe_create_indexes(&pool)
        .await
        .expect("could start index");

    let res = tx_batch(&vec_tx, &pool, 100, "test-set-wallet").await?;
    dbg!(&res);

    assert!(res.created_tx > 0);

    // check there are transaction records with function args.
    let cypher_query = neo4rs::query(
        "MATCH ()-[r:Tx]->()
        // WHERE r.args IS NOT NULL
        RETURN r
        LIMIT 1
        ",
    );

    // Execute the query
    let mut result = pool.execute(cypher_query).await?;

    // Fetch the first row only
    let row = result.next().await?;
    // let total_tx_count: i64 = row.get("total_tx_count").unwrap();
    dbg!(&row);

    Ok(())
}

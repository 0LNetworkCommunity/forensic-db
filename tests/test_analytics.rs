mod support;
use anyhow::Result;
use std::path::PathBuf;

use libra_forensic_db::{
    analytics, extract_exchange_orders, load_exchange_orders,
    neo4j_init::{get_neo4j_localhost_pool, maybe_create_indexes},
};
use support::neo4j_testcontainer::start_neo4j_container;

#[tokio::test]
async fn test_rms_single() -> Result<()> {
    libra_forensic_db::log_setup();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port).await?;
    maybe_create_indexes(&graph).await?;

    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();

    assert!(orders.len() == 25450);

    // load 1000 orders
    load_exchange_orders::swap_batch(&orders[..1000], &graph, 1000).await?;

    // get just one analytics result, never more than one (but can be empty)
    let list = analytics::exchange_stats::query_rms_analytics_chunk(&graph, 900, 1, false).await?;

    assert!(list.len() == 1);
    let first = list.first().unwrap();
    assert!(&first.time == "2024-05-15T17:41:34+00:00");
    assert!(first.matching_trades == 1);
    assert!(first.rms == 0.00403);

    Ok(())
}

#[tokio::test]
async fn test_rms_single_persist() -> Result<()> {
    libra_forensic_db::log_setup();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port).await?;
    maybe_create_indexes(&graph).await?;

    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();

    assert!(orders.len() == 25450);

    // load 1000 orders
    load_exchange_orders::swap_batch(&orders[..1000], &graph, 1000).await?;

    // get just one analytics result, never more than one (but can be empty)
    let list = analytics::exchange_stats::query_rms_analytics_chunk(&graph, 900, 1, true).await?;

    assert!(list.len() == 1);
    let first = list.first().unwrap();
    assert!(&first.time == "2024-05-15T17:41:34+00:00");
    assert!(first.matching_trades == 1);
    assert!(first.rms == 0.00403);

    Ok(())
}

#[tokio::test]
async fn test_rms_batch() -> Result<()> {
    libra_forensic_db::log_setup();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port).await?;
    maybe_create_indexes(&graph).await?;

    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();

    assert!(orders.len() == 25450);

    // load 1000 orders
    load_exchange_orders::swap_batch(&orders[..1000], &graph, 1000).await?;

    let list = analytics::exchange_stats::query_rms_analytics_concurrent(&graph, None, None, false)
        .await?;

    // NOTE: this list is incomplete, the rms is dropping
    // cases where there are no matches.

    assert!(list.len() == 800);

    Ok(())
}

mod support;
use anyhow::Result;
use std::path::PathBuf;

use libra_forensic_db::{
    analytics, extract_exchange_orders, load_exchange_orders,
    neo4j_init::{get_neo4j_localhost_pool, maybe_create_indexes},
};
use support::neo4j_testcontainer::start_neo4j_container;

// #[tokio::test]
// async fn test_rms() -> anyhow::Result<()> {
//     let c = start_neo4j_container();
//     let port = c.get_host_port_ipv4(7687);
//     let pool = get_neo4j_localhost_pool(port).await?;
//     maybe_create_indexes(&pool).await?;

//     Ok(())
// }

#[tokio::test]
async fn test_rms() -> Result<()> {
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

    let list = analytics::exchange_stats::query_rms_analytics(&graph, None).await?;
    dbg!(&list);

    // assert!(n.len() == 1000);

    Ok(())
}

mod support;

use support::neo4j_testcontainer::start_neo4j_container;

use std::path::PathBuf;

use anyhow::Result;
use libra_forensic_db::{
    analytics::{enrich_account_funding::BalanceTracker, enrich_rms},
    extract_exchange_orders, load_exchange_orders,
    neo4j_init::{get_neo4j_localhost_pool, maybe_create_indexes},
    schema_exchange_orders::ExchangeOrder,
};
use neo4rs::query;

#[test]
fn open_parse_file() {
    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();
    assert!(orders.len() == 25450);
}

#[test]
fn test_enrich_rms() {
    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let mut orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();
    assert!(orders.len() == 25450);

    enrich_rms::include_rms_stats(&mut orders);

    let count_above_100_pct = orders.iter().fold(0, |mut acc, el| {
        if el.price_vs_rms_24hour > 2.0 {
            acc += 1;
        }
        acc
    });

    assert!(count_above_100_pct == 96);

    assert!(orders.len() == 25450);
}

#[test]
fn test_sell_shill_up() {
    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let mut orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();
    assert!(orders.len() == 25450);

    enrich_rms::process_shill(&mut orders);

    let count_shill: Vec<_> = orders.iter().filter(|el| el.accepter_shill_up).collect();

    assert!(count_shill.len() == 6039);
    assert!(orders.len() == 25450);
}

#[test]
fn test_enrich_account_funding() {
    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let mut orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();

    let mut balance = BalanceTracker::new();
    balance.replay_transactions(&mut orders).unwrap();

    assert!(balance.0.len() == 3957);
}

#[test]
fn test_enrich_shill_down() {
    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let mut orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();
    assert!(orders.len() == 25450);

    enrich_rms::process_shill(&mut orders);

    let count_shill_down: Vec<_> = orders.iter().filter(|el| el.accepter_shill_down).collect();

    assert!(count_shill_down.len() == 2319);
    assert!(orders.len() == 25450);
}

#[tokio::test]
async fn test_swap_batch_cypher() -> Result<()> {
    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port).await?;
    // Three user ids exist in these two transactions
    let order1 = ExchangeOrder {
        user: 1234,
        accepter: 666,
        ..Default::default()
    };

    let order2 = ExchangeOrder {
        user: 4567,
        accepter: 666,
        ..Default::default()
    };

    let list = vec![order1.clone(), order2];
    let cypher_map = ExchangeOrder::to_cypher_map(&list);
    let insert_query = ExchangeOrder::cypher_batch_insert_str(cypher_map);

    let mut res1 = graph.execute(query(&insert_query)).await?;

    while let Some(row) = res1.next().await? {
        let count: i64 = row.get("merged_tx_count").unwrap();
        assert!(count == 2);
    }

    // now check data was loaded
    let mut result = graph
        .execute(query("MATCH (p:SwapAccount) RETURN count(p) as num"))
        .await?;

    // three accounts should have been inserted
    while let Some(row) = result.next().await? {
        let num: i64 = row.get("num").unwrap();
        assert!(num == 3);
    }

    Ok(())
}

#[tokio::test]
async fn e2e_swap_data() -> Result<()> {
    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port).await?;
    maybe_create_indexes(&graph).await?;

    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();

    assert!(orders.len() == 25450);

    // load 1000 orders
    load_exchange_orders::exchange_txs_batch(&orders[..1000], &graph, 1000).await?;

    // now check data was loaded
    let mut result = graph
        .execute(query("MATCH (p:SwapAccount) RETURN count(p) as num"))
        .await?;

    // check accounts should have been inserted
    while let Some(row) = result.next().await? {
        let num: i64 = row.get("num").unwrap();
        assert!(num == 850);
    }

    Ok(())
}

#[ignore]
#[tokio::test]
async fn test_entry_point_exchange_load() -> Result<()> {
    libra_forensic_db::log_setup();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port).await?;
    maybe_create_indexes(&graph).await?;

    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    load_exchange_orders::load_from_json(&buf, &graph, 10).await?;
    Ok(())
}

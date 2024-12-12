mod support;
use anyhow::Result;
use std::path::PathBuf;

use libra_forensic_db::{
    analytics::{
        self,
        enrich_account_funding::BalanceTracker,
        offline_matching::{self, Matching},
    },
    date_util::parse_date,
    extract_exchange_orders, load_exchange_orders,
    neo4j_init::{self, get_neo4j_localhost_pool, maybe_create_indexes},
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

#[tokio::test]
async fn test_submit_exchange_ledger() -> Result<()> {
    libra_forensic_db::log_setup();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port).await?;
    maybe_create_indexes(&graph).await?;

    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let mut orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();
    assert!(orders.len() == 25450);

    orders.retain(|el| {
        if el.filled_at < parse_date("2024-01-16") {
            if el.user == 123 {
                return true;
            };
            if el.accepter == 123 {
                return true;
            };
        }
        false
    });

    assert!(orders.len() == 68);

    let mut tracker = BalanceTracker::new();
    tracker.replay_transactions(&mut orders)?;
    dbg!(&tracker.0.len());
    let days_records = tracker.0.len();
    assert!(days_records == 47);

    let user = tracker.0.get(&123).unwrap();
    assert!(user.0.len() == 68);

    let res = tracker.submit_one_id(123, &graph).await?;

    // the number of transactions merged should equal the number of orders
    assert!(res == orders.len() as u64);

    // check there are transaction records with function args.
    let cypher_query = neo4rs::query(
        "MATCH (s:SwapAccount)-[r:DailyLedger]->(ul:UserLedger)
        WHERE s.swap_id = 123
        ORDER BY ul.date
        RETURN s.swap_id AS id, ul.date AS date, ul.total_funded AS funded
        ",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;

    let mut prev_funding = 0;
    let mut i = 0;

    // Fetch the first row only
    while let Some(r) = result.next().await? {
        if let Ok(s) = r.get::<u64>("funded") {
            i += 1;
            assert!(s >= prev_funding, "funded totals should always increase");
            prev_funding = s;
        }
    }

    assert!(i == orders.len());

    Ok(())
}

#[tokio::test]
async fn test_submit_exchange_ledger_all() -> Result<()> {
    libra_forensic_db::log_setup();

    let c = start_neo4j_container();
    let port = c.get_host_port_ipv4(7687);
    let graph = get_neo4j_localhost_pool(port).await?;
    maybe_create_indexes(&graph).await?;

    let path = env!("CARGO_MANIFEST_DIR");
    let buf = PathBuf::from(path).join("tests/fixtures/savedOlOrders2.json");
    let mut orders = extract_exchange_orders::read_orders_from_file(buf).unwrap();
    assert!(orders.len() == 25450);

    orders.retain(|el| el.filled_at < parse_date("2024-01-16"));

    assert!(orders.len() == 956);

    let mut tracker = BalanceTracker::new();
    tracker.replay_transactions(&mut orders)?;
    let days_records = tracker.0.len();
    assert!(days_records == 367); // each users * dates with txs

    let user = tracker.0.get(&123).unwrap();
    assert!(user.0.len() == 68);

    let res = tracker.submit_ledger(&graph).await?;

    // there should be double len of ledgers, since user and accepter will have a ledger
    assert!(res == (orders.len() * 2) as u64);

    // check there are transaction records with function args.
    let cypher_query = neo4rs::query(
        "MATCH (s:SwapAccount)-[r:DailyLedger]->(ul:UserLedger)
        WHERE s.swap_id = 123
        ORDER BY ul.date
        RETURN s.swap_id AS id, ul.date AS date, ul.total_funded AS funded
        ",
    );

    // Execute the query
    let mut result = graph.execute(cypher_query).await?;

    let mut prev_funding = 0;
    let mut i = 0;

    // Fetch the first row only
    while let Some(r) = result.next().await? {
        if let Ok(s) = r.get::<u64>("funded") {
            i += 1;

            assert!(s >= prev_funding, "funded totals should always increase");
            prev_funding = s;
        }
    }

    assert!(i == user.0.len());

    Ok(())
}

#[tokio::test]
async fn test_offline_analytics() -> Result<()> {
    libra_forensic_db::log_setup();
    let (uri, user, pass) = neo4j_init::get_credentials_from_env()?;
    let pool = neo4j_init::get_neo4j_remote_pool(&uri, &user, &pass).await?;

    let start_time = parse_date("2024-01-01");
    let end_time = parse_date("2024-07-10");

    let _r = offline_matching::get_exchange_users(&pool, 20, start_time, end_time).await?;

    Ok(())
}

#[tokio::test]
async fn test_offline_analytics_matching() -> Result<()> {
    libra_forensic_db::log_setup();

    let (uri, user, pass) = neo4j_init::get_credentials_from_env()?;
    let pool = neo4j_init::get_neo4j_remote_pool(&uri, &user, &pass).await?;

    let dir: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut m = Matching::read_cache_from_file(&dir).unwrap_or_default();

    let _ = m
        .depth_search_by_top_n_accounts(
            &pool,
            parse_date("2024-01-07"),
            parse_date("2024-07-22"),
            75,
            Some(dir),
        )
        .await;

    dbg!(&m.definite);

    Ok(())
}

#[tokio::test]
async fn test_easy_sellers() -> Result<()> {
    libra_forensic_db::log_setup();

    let (uri, user, pass) = neo4j_init::get_credentials_from_env()?;
    let pool = neo4j_init::get_neo4j_remote_pool(&uri, &user, &pass).await?;

    let mut user_list = offline_matching::get_exchange_users_only_outflows(&pool).await?;
    user_list
        .sort_by(|a, b: &offline_matching::MinFunding| b.funded.partial_cmp(&a.funded).unwrap());
    dbg!(&user_list.len());

    let deposits = offline_matching::get_date_range_deposits_alt(
        &pool,
        1000,
        parse_date("2024-01-07"),
        parse_date("2024-07-22"),
    )
    .await
    .unwrap_or_default();

    let dir: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut m = Matching::read_cache_from_file(&dir).unwrap_or_default();

    m.match_exact_sellers(&user_list, &deposits, 1.05);

    dbg!(&m.definite.len());

    dbg!(&m.definite);
    m.write_cache_to_file(&dir)?;

    // let _ = m
    //     .depth_search_by_top_n_accounts(
    //         &pool,
    //         parse_date("2024-01-07"),
    //         parse_date("2024-03-15"),
    //         101,
    //         Some(dir),
    //     )
    //     .await;
    // dbg!(&m.definite.len());

    Ok(())
}

#[tokio::test]
async fn test_easy_sellers_combined() -> Result<()> {
    libra_forensic_db::log_setup();

    let (uri, user, pass) = neo4j_init::get_credentials_from_env()?;
    let pool = neo4j_init::get_neo4j_remote_pool(&uri, &user, &pass).await?;

    let mut user_list = offline_matching::get_exchange_users_only_outflows(&pool).await?;
    user_list
        .sort_by(|a, b: &offline_matching::MinFunding| b.funded.partial_cmp(&a.funded).unwrap());
    // dbg!(&r[..10]);

    let deposits = offline_matching::get_date_range_deposits_alt(
        &pool,
        1000,
        parse_date("2024-01-07"),
        parse_date("2024-07-22"),
    )
    .await
    .unwrap_or_default();

    // dbg!(&deposits[..10]);

    let dir: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut m = Matching::read_cache_from_file(&dir).unwrap_or_default();

    m.match_exact_sellers(&user_list, &deposits, 1.01);

    let _ = m
        .depth_search_by_top_n_accounts(
            &pool,
            parse_date("2024-01-07"),
            parse_date("2024-07-22"),
            10,
            Some(dir),
        )
        .await;
    dbg!(&m.definite.len());

    Ok(())
}

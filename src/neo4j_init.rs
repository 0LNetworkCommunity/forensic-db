use anyhow::{Context, Result};
use neo4rs::Graph;

pub static URI_ENV: &str = "LIBRA_GRAPH_DB_URI";
pub static USER_ENV: &str = "LIBRA_GRAPH_DB_USER";
pub static PASS_ENV: &str = "LIBRA_GRAPH_DB_PASS";

pub static ACCOUNT_UNIQUE: &str =
    "CREATE CONSTRAINT unique_address IF NOT EXISTS FOR (n:Account) REQUIRE n.address IS UNIQUE";

// TODO: not null requires enterprise neo4j :/
// pub static ACCOUNT_NOT_NULL: &str =
//   "CREATE CONSTRAINT account_not_null FOR (n:Account) REQUIRE n.address IS NOT NULL";

pub static TX_CONSTRAINT: &str =
    "CREATE CONSTRAINT unique_tx_hash IF NOT EXISTS FOR ()-[r:Transfer]-() REQUIRE r.tx_hash IS UNIQUE";

// assumes the Account.address is stored as a hex string
// NOTE: hex numericals may query faster but will be hard to use in user interface
pub static INDEX_HEX_ADDR: &str =
    "CREATE TEXT INDEX hex_addr IF NOT EXISTS FOR (n:Account) ON (n.address)";

pub static INDEX_TX_TIMESTAMP: &str =
    "CREATE INDEX tx_timestamp IF NOT EXISTS FOR ()-[r:Tx]-() ON (r.block_datetime)";

pub static INDEX_TX_HASH: &str =
    "CREATE INDEX tx_function IF NOT EXISTS FOR ()-[r:Tx]-() ON (r.tx_hash)";

pub static INDEX_TX_AMOUNT: &str =
    "CREATE INDEX tx_function IF NOT EXISTS FOR ()-[r:Tx]-() ON (r.amount)";

pub static INDEX_TX_FRAMEWORK: &str =
    "CREATE INDEX tx_function IF NOT EXISTS FOR ()-[r:Tx]-() ON (r.framework_version)";

pub static INDEX_TX_RELATION: &str =
    "CREATE INDEX tx_relation IF NOT EXISTS FOR ()-[r:Tx]-() ON (r.relation)";

pub static INDEX_SWAP_ID: &str =
    "CREATE INDEX swap_account_id IF NOT EXISTS FOR (n:SwapAccount) ON (n.swap_id)";

pub static INDEX_SWAP_TIME: &str =
    "CREATE INDEX swap_time IF NOT EXISTS FOR ()-[r:Swap]-() ON (r.filled_at)";

pub static INDEX_EXCHANGE_LEDGER: &str = "
    CREATE INDEX user_ledger IF NOT EXISTS FOR (ul:UserLedger) ON (ul.date)
    ";

pub static INDEX_EXCHANGE_LINK_LEDGER: &str = "
    CREATE INDEX link_ledger IF NOT EXISTS FOR ()-[r:DailyLedger]->() ON (r.date)
    ";

pub static INDEX_LIFETIME: &str = "
    CREATE INDEX link_ledger IF NOT EXISTS FOR ()-[r:Lifetime]->() ON (r.amount)
    ";

pub static INDEX_SNAPSHOT: &str = "CREATE INDEX snapshot_account_id IF NOT EXISTS FOR (n:Snapshot) ON (n.address, n.epoch, n.version)";
/// get the testing neo4j connection
pub async fn get_neo4j_localhost_pool(port: u16) -> Result<Graph> {
    let uri = format!("127.0.0.1:{port}");
    let user = "neo4j";
    let pass = "neo";
    Ok(Graph::new(uri, user, pass).await?)
}

/// get the driver connection object
pub async fn get_neo4j_remote_pool(uri: &str, user: &str, pass: &str) -> Result<Graph> {
    Ok(Graph::new(uri, user, pass).await?)
}

pub fn get_credentials_from_env() -> Result<(String, String, String)> {
    let uri = std::env::var(URI_ENV).context(format!("could not get env var {}", URI_ENV))?;
    let user = std::env::var(USER_ENV).context(format!("could not get env var {}", USER_ENV))?;
    let pass = std::env::var(PASS_ENV).context(format!("could not get env var {}", PASS_ENV))?;

    Ok((uri, user, pass))
}

pub async fn maybe_create_indexes(graph: &Graph) -> Result<()> {
    let mut txn = graph.start_txn().await?;

    txn.run_queries([
        ACCOUNT_UNIQUE,
        TX_CONSTRAINT,
        INDEX_HEX_ADDR,
        INDEX_TX_TIMESTAMP,
        INDEX_TX_HASH,
        INDEX_TX_AMOUNT,
        INDEX_TX_FRAMEWORK,
        INDEX_TX_RELATION,
        INDEX_SWAP_ID,
        INDEX_EXCHANGE_LEDGER,
        INDEX_EXCHANGE_LINK_LEDGER,
        INDEX_LIFETIME,
        INDEX_SNAPSHOT,
    ])
    .await?;
    txn.commit().await?;
    Ok(())
}

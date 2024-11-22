pub mod batch_tx_type;
pub mod cypher_templates;
pub mod enrich_exchange_onboarding;
pub mod enrich_whitepages;
pub mod extract_snapshot;
pub mod extract_transactions;
pub mod load;
pub mod load_account_state;
pub mod load_exchange_orders;
pub mod load_tx_cypher;
pub mod neo4j_init;
pub mod queue;
pub mod scan;
pub mod schema_account_state;
pub mod schema_exchange_orders;
pub mod schema_transaction;
pub mod unzip_temp;
pub mod warehouse_cli;

use std::sync::Once;

static LOGGER: Once = Once::new();

/// Setup function that is only run once, even if called multiple times.
pub fn log_setup() {
    LOGGER.call_once(|| {
        env_logger::init();
    });
}

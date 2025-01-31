pub mod analytics;
pub mod batch_tx_type;
pub mod cypher_templates;
pub mod decode_entry_function;
pub mod enrich_exchange_onboarding;
pub mod enrich_whitepages;
pub mod extract_exchange_orders;
pub mod extract_snapshot;
pub mod extract_transactions;
pub mod json_rescue_v5_extract;
pub mod json_rescue_v5_load;
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
pub mod util;
pub mod v5_rpc_to_raw;
pub mod warehouse_cli;

use std::sync::Once;

use env_logger::Env;

static LOGGER: Once = Once::new();

/// Setup function that is only run once, even if called multiple times.
pub fn log_setup() {
    LOGGER.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    });
}

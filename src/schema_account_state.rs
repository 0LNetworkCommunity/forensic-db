use libra_types::exports::AccountAddress;

use crate::scan::FrameworkVersion;

// holds timestamp, chain height, and epoch
#[derive(Debug, Clone, Default)]
pub struct WarehouseTime {
    pub framework_version: FrameworkVersion,
    pub timestamp: u64,
    pub version: u64,
    pub epoch: u64,
}
#[derive(Debug, Clone)]
/// The basic information for an account
pub struct WarehouseAccState {
    pub address: AccountAddress,
    pub time: WarehouseTime,
    pub sequence_num: u64,
    pub balance: f64,
    pub slow_wallet_unlocked: Option<f64>,
    pub slow_wallet_transferred: Option<f64>,
    pub slow_wallet_acc: bool,
    pub donor_voice_acc: bool,
    pub miner_height: Option<u64>,
}

impl Default for WarehouseAccState {
    fn default() -> Self {
        Self {
            address: AccountAddress::ZERO,
            sequence_num: 0,
            balance: 0.0,
            slow_wallet_unlocked: None,
            slow_wallet_transferred: None,
            slow_wallet_acc: false,
            donor_voice_acc: false,
            miner_height: None,
            time: WarehouseTime::default(),
        }
    }
}

impl WarehouseAccState {
    pub fn new(address: AccountAddress) -> Self {
        Self {
            address,
            ..Default::default()
        }
    }
    pub fn set_time(&mut self, timestamp: u64, version: u64, epoch: u64) {
        self.time.timestamp = timestamp;
        self.time.version = version;
        self.time.epoch = epoch;
    }
}

impl WarehouseAccState {
    /// creates one transaction record in the cypher query map format
    /// Note original data was in an RFC rfc3339 with Z for UTC, Cypher seems to prefer with offsets +00000
    pub fn acc_state_to_cypher_map(&self) -> String {
        let slow_wallet_unlocked_literal = match self.slow_wallet_unlocked {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        };
        let slow_wallet_transferred_literal = match self.slow_wallet_transferred {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        };

        let miner_height_literal = match self.miner_height {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        };

        format!(
            r#"{{address: "{}", balance: {}, version: {}, epoch: {},sequence_num: {}, slow_unlocked: {}, slow_transfer: {}, framework_version: "{}", slow_wallet: {}, donor_voice: {}, miner_height: {}}}"#,
            self.address.to_hex_literal(),
            self.balance,
            self.time.version,
            self.time.epoch,
            self.sequence_num,
            slow_wallet_unlocked_literal,
            slow_wallet_transferred_literal,
            self.time.framework_version,
            self.slow_wallet_acc,
            self.donor_voice_acc,
            miner_height_literal
        )
    }

    /// create a cypher query string for the map object
    pub fn to_cypher_map(list: &[Self]) -> String {
        let mut list_literal = "".to_owned();
        for el in list {
            let s = el.acc_state_to_cypher_map();
            list_literal.push_str(&s);
            list_literal.push(',');
        }
        list_literal.pop(); // need to drop last comma ","
        format!("[{}]", list_literal)
    }

    pub fn cypher_batch_insert_str(list_str: &str) -> String {
        format!(
            r#"
WITH {list_str} AS tx_data
UNWIND tx_data AS tx

MERGE (addr:Account {{address: tx.address}})
MERGE (snap:Snapshot {{
    address: tx.address,
    epoch: tx.epoch,
    version: tx.version
}})

SET
  snap.balance = tx.balance,
  snap.framework_version = tx.framework_version,
  snap.sequence_num = tx.sequence_num,
  snap.slow_wallet = tx.slow_wallet,
  snap.donor_voice = tx.donor_voice

// Conditionally add `tx.miner_height` if it exists
FOREACH (_ IN CASE WHEN tx.miner_height IS NOT NULL THEN [1] ELSE [] END |
    SET snap.miner_height = tx.miner_height
)

// Conditionally add `tx.slow_unlocked` if it exists
FOREACH (_ IN CASE WHEN tx.slow_unlocked IS NOT NULL THEN [1] ELSE [] END |
    SET snap.slow_unlocked = tx.slow_unlocked
)

// Conditionally add `tx.slow_transfer` if it exists
FOREACH (_ IN CASE WHEN tx.slow_transfer IS NOT NULL THEN [1] ELSE [] END |
    SET snap.slow_transfer = tx.slow_transfer
)

MERGE (addr)-[rel:State {{version: tx.version}}]->(snap)

RETURN COUNT(snap) AS merged_snapshots

"#
        )
    }
}

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
    pub balance: u64,
    pub slow_wallet_unlocked: Option<u64>,
    pub slow_wallet_transferred: Option<u64>,
    pub slow_wallet_acc: bool,
    pub donor_voice_acc: bool,
    pub miner_height: Option<u64>,
}

impl Default for WarehouseAccState {
    fn default() -> Self {
        Self {
            address: AccountAddress::ZERO,
            sequence_num: 0,
            balance: 0,
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
    pub fn to_cypher_object_template(&self) -> String {
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
            let s = el.to_cypher_object_template();
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
    balance: tx.balance,
    epoch: tx.epoch,
    framework_version: tx.framework_version,
    version: tx.version,
    sequence_num: tx.sequence_num,
    slow_unlocked: tx.slow_unlocked,
    slow_transfer: tx.slow_transfer,
    slow_wallet: tx.slow_wallet,
    donor_voice: tx.donor_voice,
    miner_height: coalesce(tx.miner_height, 0)
}})
MERGE (addr)-[rel:State {{version: tx.version}}]->(snap)

RETURN COUNT(snap) AS merged_snapshots

"#
        )
    }
}

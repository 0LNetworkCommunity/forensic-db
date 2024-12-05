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
    pub slow_wallet_locked: u64,
    pub slow_wallet_transferred: u64,
    pub donor_voice_acc: bool,
}

impl Default for WarehouseAccState {
    fn default() -> Self {
        Self {
            address: AccountAddress::ZERO,
            time: Default::default(),
            sequence_num: Default::default(),
            balance: Default::default(),
            slow_wallet_locked: Default::default(),
            slow_wallet_transferred: Default::default(),
            donor_voice_acc: false,
        }
    }
}

impl WarehouseAccState {
    pub fn new(address: AccountAddress) -> Self {
        Self {
            address,
            sequence_num: 0,
            time: WarehouseTime::default(),
            balance: 0,
            slow_wallet_locked: 0,
            slow_wallet_transferred: 0,
            donor_voice_acc: false,
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
        format!(
            r#"{{address: "{}", balance: {}, version: {}, sequence_num: {}, slow_locked: {}, slow_transfer: {}, framework_version: "{}", donor_voice: {} }}"#,
            self.address.to_hex_literal(),
            self.balance,
            self.time.version,
            self.sequence_num,
            self.slow_wallet_locked,
            self.slow_wallet_transferred,
            self.time.framework_version,
            self.donor_voice_acc,
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
  MERGE (snap:Snapshot {{address: tx.address, balance: tx.balance, framework_version: tx.framework_version, version: tx.version, sequence_num: tx.sequence_num, slow_locked: tx.slow_locked, slow_transfer: tx.slow_transfer, donor_voice: tx.donor_voice }})
  MERGE (addr)-[rel:State {{version: tx.version}} ]->(snap)

  RETURN
      COUNT(snap) AS merged_snapshots

"#
        )
    }
}

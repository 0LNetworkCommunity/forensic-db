use libra_types::exports::AccountAddress;

#[derive(Debug, Clone)]
/// The basic information for an account
pub struct WarehouseAccState {
    pub address: AccountAddress,
    pub time: WarehouseTime,
    pub sequence_num: u64,
    pub balance: u64,
    pub slow_wallet_locked: u64,
    pub slow_wallet_transferred: u64,
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
        }
    }
    pub fn set_time(&mut self, timestamp: u64, version: u64, epoch: u64) {
        self.time.timestamp = timestamp;
        self.time.version = version;
        self.time.epoch = epoch;
    }
}
// holds timestamp, chain height, and epoch
#[derive(Debug, Clone, Default)]
pub struct WarehouseTime {
    pub timestamp: u64,
    pub version: u64,
    pub epoch: u64,
}

impl WarehouseAccState {
    /// creates one transaction record in the cypher query map format
    /// Note original data was in an RFC rfc3339 with Z for UTC, Cypher seems to prefer with offsets +00000
    pub fn to_cypher_object_template(&self) -> String {
        format!(
            r#"{{address: {}, balance: {} }}"#,
            self.address,
            self.balance,
            // self.order_type,
            // self.amount,
            // self.price,
            // self.created_at.to_rfc3339(),
            // self.created_at.timestamp_micros(),
            // self.filled_at.to_rfc3339(),
            // self.filled_at.timestamp_micros()
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

  MATCH (addr:Account {{address: tx.address}})

  MERGE (snap:Snapshot {{address: tx.address, balance: tx.balance }})
  MERGE (addr)-[rel:State]->(snap)

  RETURN
      COUNT(snap) AS merged_snapshots

"#
        )
    }
}

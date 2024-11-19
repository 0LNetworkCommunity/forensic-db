use crate::cypher_templates::to_cypher_object;

use chrono::{DateTime, Utc};
use diem_crypto::HashValue;
use diem_types::account_config::{DepositEvent, WithdrawEvent};
use libra_backwards_compatibility::sdk::v7_libra_framework_sdk_builder::EntryFunctionCall;
use libra_types::{exports::AccountAddress, move_resource::coin_register_event::CoinRegisterEvent};
use neo4rs::{BoltList, BoltMap, BoltType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelationLabel {
    Tx, // undefined tx
    Transfer(AccountAddress),
    Onboarding(AccountAddress),
    Vouch(AccountAddress),
    Configuration,
    Miner,
    // Script,
    // MiscEntryFunction,
}

impl RelationLabel {
    pub fn to_cypher_label(&self) -> String {
        match self {
            RelationLabel::Tx => "Tx".to_owned(),
            RelationLabel::Transfer(_) => "Tx".to_owned(),
            RelationLabel::Onboarding(_) => "Onboarding".to_owned(),
            RelationLabel::Vouch(_) => "Vouch".to_owned(),
            RelationLabel::Configuration => "Configuration".to_owned(),
            RelationLabel::Miner => "Miner".to_owned(),
        }
    }

    pub fn get_recipient(&self) -> Option<AccountAddress> {
        match &self {
            RelationLabel::Tx => None,
            RelationLabel::Transfer(account_address) => Some(*account_address),
            RelationLabel::Onboarding(account_address) => Some(*account_address),
            RelationLabel::Vouch(account_address) => Some(*account_address),
            RelationLabel::Configuration => None,
            RelationLabel::Miner => None,
        }
    }
}

// TODO: deprecate?
#[derive(Debug, Clone)]
pub struct TransferTx {
    pub tx_hash: HashValue,
    pub to: AccountAddress,
    pub amount: u64,
}

// TODO: deprecate?
#[derive(Debug, Clone)]
pub struct MiscTx {
    pub tx_hash: HashValue, // primary key
    pub data: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WarehouseEvent {
    pub tx_hash: HashValue, // primary key
    pub event: UserEventTypes,
    pub event_name: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]

pub enum UserEventTypes {
    Withdraw(WithdrawEvent),
    Deposit(DepositEvent),
    Onboard(CoinRegisterEvent),
    Other,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub enum EntryFunctionArgs {
    V7(EntryFunctionCall),
    // TODO:
    // V6(V6EntryFunctionCall),
    // V5(V5EntryFunctionCall),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WarehouseTxMaster {
    pub tx_hash: HashValue, // primary key
    pub relation_label: RelationLabel,
    pub sender: AccountAddress,
    pub function: String,
    pub epoch: u64,
    pub round: u64,
    pub block_timestamp: u64,
    pub block_datetime: DateTime<Utc>,
    pub expiration_timestamp: u64,
    pub entry_function: Option<EntryFunctionArgs>,
    pub events: Vec<WarehouseEvent>,
}

impl Default for WarehouseTxMaster {
    fn default() -> Self {
        Self {
            tx_hash: HashValue::zero(),
            relation_label: RelationLabel::Configuration,
            sender: AccountAddress::ZERO,
            function: "none".to_owned(),
            epoch: 0,
            round: 0,
            block_timestamp: 0,
            block_datetime: DateTime::<Utc>::from_timestamp_micros(0).unwrap(),
            expiration_timestamp: 0,
            entry_function: None,
            // args: json!(""),
            events: vec![],
        }
    }
}

impl WarehouseTxMaster {
    /// since no sane Cypher serialization libraries exist.
    /// and I'm not going to write a deserializer.
    /// and JSON is not the same format as cypher property maps
    /// JSON5 but the last time someone updated
    /// that crate was 3 years ago.
    pub fn to_cypher_object_template(&self) -> String {
        let tx_args = match &self.entry_function {
            Some(ef) => to_cypher_object(ef, None).unwrap_or("{test: 0}".to_string()),
            None => "{test: 1}".to_owned(),
        };

        format!(
            r#"{{tx_hash: "{}", block_datetime: datetime("{}"), block_timestamp: {}, relation: "{}", function: "{}", sender: "{}", args: {}, recipient: "{}"}}"#,
            self.tx_hash.to_hex_literal(),
            self.block_datetime.to_rfc3339(),
            self.block_timestamp,
            self.relation_label.to_cypher_label(),
            self.function,
            self.sender.to_hex_literal(),
            tx_args,
            self.relation_label
                .get_recipient()
                .unwrap_or(self.sender)
                .to_hex_literal(),
        )
    }

    /// make a string from the warehouse object
    pub fn to_cypher_map(txs: &[Self]) -> String {
        let mut list_literal = "".to_owned();
        for el in txs {
            let s = el.to_cypher_object_template();
            list_literal.push_str(&s);
            list_literal.push(',');
        }
        list_literal.pop(); // need to drop last comma ","
        format!("[{}]", list_literal)
    }

    // NOTE: this seems to be memory inefficient.
    // also creates a vendor lock-in with neo4rs instead of any open cypher.
    // Hence the query templating
    pub fn to_boltmap(&self) -> BoltMap {
        let mut map = BoltMap::new();
        map.put("tx_hash".into(), self.tx_hash.to_string().into());
        map.put("sender".into(), self.sender.clone().to_hex_literal().into());
        map.put(
            "recipient".into(),
            self.sender.clone().to_hex_literal().into(),
        );

        // TODO
        // map.put("epoch".into(), self.epoch.into());
        // map.put("round".into(), self.round.into());
        // map.put("epoch".into(), self.epoch.into());
        // map.put("block_timestamp".into(), self.block_timestamp.into());
        // map.put(
        //     "expiration_timestamp".into(),
        //     self.expiration_timestamp.into(),
        // );
        map
    }
    /// how one might implement the bolt types.
    pub fn slice_to_bolt_list(txs: &[Self]) -> BoltType {
        let mut list = BoltList::new();
        for el in txs {
            let map = el.to_boltmap();
            list.push(BoltType::Map(map));
        }
        BoltType::List(list)
    }
}

#[derive(Debug, Clone)]
/// The basic information for an account
pub struct WarehouseRecord {
    pub account: WarehouseAccount,
    pub time: WarehouseTime,
    pub balance: Option<WarehouseBalance>,
}

impl WarehouseRecord {
    pub fn new(address: AccountAddress) -> Self {
        Self {
            account: WarehouseAccount { address },
            time: WarehouseTime::default(),
            balance: Some(WarehouseBalance::default()),
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
#[derive(Debug, Clone)]
pub struct WarehouseAccount {
    pub address: AccountAddress,
}

#[derive(Debug, Default, Clone)]
pub struct WarehouseBalance {
    // balances in v6+ terms
    pub balance: u64,
}

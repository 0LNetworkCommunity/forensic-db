use crate::cypher_templates::to_cypher_object;

use chrono::{DateTime, Utc};
use diem_crypto::HashValue;
use diem_types::account_config::{DepositEvent, WithdrawEvent};
use libra_backwards_compatibility::sdk::v7_libra_framework_sdk_builder::EntryFunctionCall;
use libra_types::{exports::AccountAddress, move_resource::coin_register_event::CoinRegisterEvent};
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

#[derive(Debug, Serialize, Deserialize)]
pub struct WarehouseEvent {
    pub tx_hash: HashValue,
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
    pub tx_hash: HashValue,
    pub relation_label: RelationLabel,
    pub sender: AccountAddress,
    pub recipient: Option<AccountAddress>,
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
            recipient: Some(AccountAddress::ZERO),
            function: "none".to_owned(),
            epoch: 0,
            round: 0,
            block_timestamp: 0,
            block_datetime: DateTime::<Utc>::from_timestamp_micros(0).unwrap(),
            expiration_timestamp: 0,
            entry_function: None,
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
            self.recipient.unwrap_or(self.sender).to_hex_literal(),
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
}

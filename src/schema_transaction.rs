use crate::{cypher_templates::to_cypher_object, scan::FrameworkVersion};

use chrono::{DateTime, Utc};
use diem_crypto::HashValue;
use diem_types::account_config::{DepositEvent, WithdrawEvent};
use libra_backwards_compatibility::sdk::{
    v5_0_0_genesis_transaction_script_builder::ScriptFunctionCall as ScriptFunctionCallGenesis,
    v5_2_0_transaction_script_builder::ScriptFunctionCall as ScriptFunctionCallV520,
    v6_libra_framework_sdk_builder::EntryFunctionCall as V6EntryFunctionCall,
    v7_libra_framework_sdk_builder::EntryFunctionCall as V7EntryFunctionCall,
};
use libra_types::{exports::AccountAddress, move_resource::coin_register_event::CoinRegisterEvent};
use serde::{Deserialize, Serialize};

// TODO check decimal precision
/// Conversion of coins from V5 to V6
pub const LEGACY_REBASE_MULTIPLIER: u64 = 35;
/// Decimal precision
// TODO: duplication, this is probably defined in libra-framework somewhere
pub const COIN_DECIMAL_PRECISION: f64 = 1000000.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RelationLabel {
    Unknown, // undefined tx
    // NOTE: amount u64 includes:
    // - legacy multiplier in v6 rebase (for all pre-v6 data)
    // - decimal precision scaling
    Transfer(AccountAddress, u64),
    Onboarding(AccountAddress, u64),
    Vouch(AccountAddress),
    Configuration,
    Miner,
}

impl RelationLabel {
    pub fn to_cypher_label(&self) -> String {
        match self {
            RelationLabel::Unknown => "Unknown".to_owned(),
            RelationLabel::Transfer(..) => "Transfer".to_owned(),
            RelationLabel::Onboarding(..) => "Onboarding".to_owned(),
            RelationLabel::Vouch(..) => "Vouch".to_owned(),
            RelationLabel::Configuration => "Configuration".to_owned(),
            RelationLabel::Miner => "Miner".to_owned(),
        }
    }

    pub fn get_recipient(&self) -> Option<AccountAddress> {
        match &self {
            RelationLabel::Unknown => None,
            RelationLabel::Transfer(account_address, _) => Some(*account_address),
            RelationLabel::Onboarding(account_address, _) => Some(*account_address),
            RelationLabel::Vouch(account_address) => Some(*account_address),
            RelationLabel::Configuration => None,
            RelationLabel::Miner => None,
        }
    }

    pub fn get_coins_human_readable(&self) -> Option<f64> {
        match &self {
            RelationLabel::Transfer(_, amount) => {
                if *amount > 0 {
                    let human = (*amount as f64) / COIN_DECIMAL_PRECISION;
                    return Some(human);
                }
            }
            RelationLabel::Onboarding(_, amount) => {
                if *amount > 0 {
                    let human = (*amount as f64) / COIN_DECIMAL_PRECISION;
                    return Some(human);
                }
            }
            _ => {}
        }
        None
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
    // TODO:
    // Current(CurrentVersionEntryFunctionCall),
    V7(V7EntryFunctionCall),
    V6(V6EntryFunctionCall),
    V5(ScriptFunctionCallGenesis),
    V520(ScriptFunctionCallV520),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WarehouseTxMaster {
    pub tx_hash: HashValue,
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
    pub framework_version: FrameworkVersion,
    // TODO framework version
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
            events: vec![],
            framework_version: FrameworkVersion::Unknown,
        }
    }
}

impl WarehouseTxMaster {
    /// since no sane Cypher serialization libraries exist.
    /// and I'm not going to write a deserializer.
    /// and JSON is not the same format as cypher property maps
    /// I'd use JSON5 but the last time someone updated
    /// that crate was 3 years ago.
    pub fn to_cypher_object_template(&self) -> String {
        // make blank string or nest the arguments
        let mut tx_args = "NULL".to_string();
        if let Some(args) = &self.entry_function {
            if let Ok(st) = to_cypher_object(args) {
                tx_args = st;
            }
        };
        let mut coins_literal = "NULL".to_string();
        if let Some(c) = &self.relation_label.get_coins_human_readable() {
            coins_literal = format!("{:.2}", c);
        };
        format!(
            r#"{{ args: {tx_args}, coins: {coins_literal}, tx_hash: "{}", block_datetime: datetime("{}"), block_timestamp: {}, relation: "{}", function: "{}", sender: "{}", recipient: "{}", framework_version: "{}"}}"#,
            self.tx_hash.to_hex_literal(),
            self.block_datetime.to_rfc3339(),
            self.block_timestamp,
            self.relation_label.to_cypher_label(),
            self.function,
            self.sender.to_hex_literal(),
            self.relation_label
                .get_recipient()
                .unwrap_or(self.sender)
                .to_hex_literal(),
            self.framework_version
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

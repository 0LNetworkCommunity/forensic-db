use crate::{
    json_rescue_v5_compat::TransactionViewV5,
    schema_transaction::{WarehouseEvent, WarehouseTxMaster},
};
use anyhow::Result;
use std::path::Path;

/// The canonical transaction archives for V5 were kept in a different format as in v6 and v7.
/// As of Nov 2024, there's a project to recover the V5 transaction archives to be in the same bytecode flat file format as v6 and v7.
/// Until then, we must parse the json files.

pub fn extract_v5_json_rescue(
    one_json_file: &Path,
) -> Result<(Vec<WarehouseTxMaster>, Vec<WarehouseEvent>)> {
    dbg!(&one_json_file);
    let json = std::fs::read_to_string(one_json_file)?;

    let txs: Vec<TransactionViewV5> = serde_json::from_str(&json)?;
    dbg!(&txs);
    todo!()
}

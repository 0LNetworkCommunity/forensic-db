use crate::{
    schema_transaction::{EntryFunctionArgs, RelationLabel, WarehouseEvent, WarehouseTxMaster},
    unzip_temp::decompress_tar_archive,
};
use libra_backwards_compatibility::{
    sdk::v5_0_0_genesis_transaction_script_builder::ScriptFunctionCall as ScriptFunctionCallGenesis,
    sdk::v5_2_0_transaction_script_builder::ScriptFunctionCall as ScriptFunctionCallV520,
    version_five::{
        transaction_type_v5::{TransactionPayload, TransactionV5},
        transaction_view_v5::{ScriptView, TransactionDataView, TransactionViewV5},
    },
};

use anyhow::{anyhow, Context, Result};
use diem_temppath::TempPath;
use diem_types::account_address::AccountAddress;
use log::info;
use std::path::{Path, PathBuf};
/// The canonical transaction archives for V5 were kept in a different format as in v6 and v7.
/// As of Nov 2024, there's a project to recover the V5 transaction archives to be in the same bytecode flat file format as v6 and v7.
/// Until then, we must parse the json files.

pub fn extract_v5_json_rescue(
    one_json_file: &Path,
) -> Result<(Vec<WarehouseTxMaster>, Vec<WarehouseEvent>)> {
    let json = std::fs::read_to_string(one_json_file).context("could not read file")?;

    let txs: Vec<TransactionViewV5> = serde_json::from_str(&json)
        .map_err(|e| anyhow!("could not parse JSON to TransactionViewV5, {:?}", e))?;

    let mut tx_vec = vec![];
    let event_vec = vec![];

    for t in txs {
        let mut wtxs = WarehouseTxMaster::default();
        match &t.transaction {
            TransactionDataView::UserTransaction { sender, script, .. } => {
                // dbg!(&t);
                wtxs.sender = AccountAddress::from_hex_literal(&sender.to_hex_literal())?;

                // wtxs.tx_hash = HashValue::from_str(&t.hash.to_hex_literal())?;

                wtxs.function = make_function_name(script);
                info!("function: {}", &wtxs.function);

                wtxs.relation_label = guess_relation(&wtxs.function);

                // wtxs.events
                // wtxs.block_timestamp
                wtxs.entry_function = decode_transaction_args(&t.bytes);

                tx_vec.push(wtxs);
            }
            TransactionDataView::BlockMetadata { timestamp_usecs: _ } => {
                // TODO get epoch events
                // todo!();
                //  t.events.iter().any(|e|{
                // if let epoch: NewEpoch = e.data {
                //   }
                // })
            }
            _ => {}
        }
    }

    Ok((tx_vec, event_vec))
}

pub fn decode_transaction_args(tx_bytes: &[u8]) -> Option<EntryFunctionArgs> {
    // test we can bcs decode to the transaction object
    let t: TransactionV5 = bcs::from_bytes(tx_bytes).unwrap();

    if let TransactionV5::UserTransaction(u) = &t {
        match &u.raw_txn.payload {
            TransactionPayload::ScriptFunction(_) => {
                info!("ScriptFunction");

                if let Some(sf) = ScriptFunctionCallGenesis::decode(&u.raw_txn.payload) {
                    dbg!("genesis", &sf);
                    Some(EntryFunctionArgs::V5(sf))
                } else if let Some(sf) = ScriptFunctionCallV520::decode(&u.raw_txn.payload) {
                    dbg!("520", &sf);
                    Some(EntryFunctionArgs::V520(sf))
                }
            }
            _ => None,
        }
    }
}
/// from a tgz file unwrap to temp path
/// NOTE: we return the Temppath object for the directory
/// for the enclosing function to handle
/// since it will delete all the files once it goes out of scope.
pub fn decompress_to_temppath(tgz_file: &Path) -> Result<TempPath> {
    let temp_dir = TempPath::new();
    temp_dir.create_as_dir()?;

    decompress_tar_archive(tgz_file, temp_dir.path())?;

    Ok(temp_dir)
}

/// gets all json files decompressed from tgz
pub fn list_all_json_files(search_dir: &Path) -> Result<Vec<PathBuf>> {
    let path = search_dir.canonicalize()?;

    let pattern = format!(
        "{}/**/*.json",
        path.to_str().context("cannot parse starting dir")?
    );

    let vec_pathbuf = glob::glob(&pattern)?.map(|el| el.unwrap()).collect();
    Ok(vec_pathbuf)
}

// TODO: gross borrows, lazy.
fn make_function_name(script: &ScriptView) -> String {
    let module = script.module_name.as_ref();

    let function = script.function_name.as_ref();

    format!(
        "0x::{}::{}",
        module.unwrap_or(&"none".to_string()),
        function.unwrap_or(&"none".to_string())
    )
}

fn guess_relation(script_name: &str) -> RelationLabel {
    if script_name.contains("minerstate_commit") {
        RelationLabel::Miner
    } else if script_name.contains("create_user_by_coin_tx") {
        // TODO: get the address
        RelationLabel::Onboarding(AccountAddress::ZERO)
    } else if script_name.contains("set_wallet_type") {
        RelationLabel::Configuration
    } else {
        dbg!(&script_name);
        RelationLabel::Tx
    }
}

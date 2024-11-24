use crate::{
    json_rescue_v5_compat::{ScriptView, TransactionDataView, TransactionViewV5},
    schema_transaction::{RelationLabel, WarehouseEvent, WarehouseTxMaster},
    unzip_temp::decompress_tar_archive,
};
use anyhow::{anyhow, Context, Result};
use diem_temppath::TempPath;
use diem_types::account_address::AccountAddress;
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
        // if let Some(UserTransaction {}) == &t {
        //   dbg!(&t);
        // }
        let mut wtxs = WarehouseTxMaster::default();
        match &t.transaction {
            TransactionDataView::UserTransaction { sender, script, .. } => {
                // dbg!(&t);
                wtxs.sender = AccountAddress::from_hex_literal(&sender.to_hex_literal())?;
                wtxs.tx_hash = t.hash;

                wtxs.function = make_function_name(script);

                wtxs.relation_label = guess_relation(&wtxs.function);
                // wtxs.events
                // wtxs.block_timestamp
                // wtxs.entry_function
                // wtxs.

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

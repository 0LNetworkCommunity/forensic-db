use crate::{
    scan::FrameworkVersion,
    schema_transaction::{EntryFunctionArgs, RelationLabel, WarehouseEvent, WarehouseTxMaster},
    unzip_temp::decompress_tar_archive,
    util::{COIN_DECIMAL_PRECISION, LEGACY_REBASE_MULTIPLIER},
};
use chrono::DateTime;
use diem_crypto::HashValue;
use libra_backwards_compatibility::{
    sdk::{
        v5_0_0_genesis_transaction_script_builder::ScriptFunctionCall as ScriptFunctionCallGenesis,
        v5_2_0_transaction_script_builder::ScriptFunctionCall as ScriptFunctionCallV520,
    },
    version_five::{
        legacy_address_v5::LegacyAddressV5,
        transaction_type_v5::{TransactionPayload, TransactionV5},
        transaction_view_v5::{ScriptView, TransactionDataView, TransactionViewV5},
    },
};

use anyhow::{anyhow, Context, Result};
use diem_temppath::TempPath;
use diem_types::account_address::AccountAddress;
use log::trace;
use std::path::{Path, PathBuf};

/// The canonical transaction archives for V5 were kept in a different format as in v6 and v7.
/// As of Nov 2024, there's a project to recover the V5 transaction archives to be in the same bytecode flat file format as v6 and v7.
/// Until then, we must parse the json files.
pub fn extract_v5_json_rescue(
    one_json_file: &Path,
) -> Result<(Vec<WarehouseTxMaster>, Vec<WarehouseEvent>, Vec<String>)> {
    let json = std::fs::read_to_string(one_json_file).context("could not read file")?;

    let txs: Vec<TransactionViewV5> = serde_json::from_str(&json)
        .map_err(|e| anyhow!("could not parse JSON to TransactionViewV5, {:?}", e))?;

    decode_transaction_dataview_v5(&txs)
}

pub fn decode_transaction_dataview_v5(
    txs: &[TransactionViewV5],
) -> Result<(Vec<WarehouseTxMaster>, Vec<WarehouseEvent>, Vec<String>)> {
    let mut tx_vec = vec![];
    let event_vec = vec![];
    let mut unique_functions = vec![];

    for t in txs {
        let mut wtxs = WarehouseTxMaster {
            framework_version: FrameworkVersion::V5,
            ..Default::default()
        };

        let timestamp = t.timestamp_usecs.unwrap_or(0);
        if let TransactionDataView::UserTransaction { sender, script, .. } = &t.transaction {
            wtxs.sender = cast_legacy_account(sender)?;

            // must cast from V5 HashValue buffer layout
            wtxs.tx_hash = HashValue::from_slice(t.hash.to_vec())?;

            wtxs.function = make_function_name(script);
            trace!("function: {}", &wtxs.function);
            if !unique_functions.contains(&wtxs.function) {
                unique_functions.push(wtxs.function.clone());
            }

            decode_entry_function_v5(&mut wtxs, &t.bytes)?;

            // TODO: EPOCH does not exist in v5 rescue json transaction record
            // each .json is not guaranteed to have an epoch change event.
            // tracking epoch change events and incrementing is error prone
            // as the async loader does not guarantee ordered reading of files.
            wtxs.block_timestamp = timestamp;
            wtxs.block_datetime =
                DateTime::from_timestamp_micros(timestamp as i64).expect("get timestamp");

            match &wtxs.relation_label {
                RelationLabel::Unknown => {}
                RelationLabel::Transfer(..) => tx_vec.push(wtxs),
                RelationLabel::Onboarding(..) => tx_vec.push(wtxs),
                RelationLabel::Vouch(..) => tx_vec.push(wtxs),
                RelationLabel::Configuration => {}
                RelationLabel::Miner => {}
            };
        }
    }
    Ok((tx_vec, event_vec, unique_functions))
}

pub fn decode_entry_function_v5(wtx: &mut WarehouseTxMaster, tx_bytes: &[u8]) -> Result<()> {
    // test we can bcs decode to the transaction object
    let t: TransactionV5 = bcs::from_bytes(tx_bytes).map_err(|err| {
        anyhow!(
            "could not bcs decode tx_bytes, for function: {}, msg: {:?}",
            wtx.function,
            err
        )
    })?;

    if let TransactionV5::UserTransaction(u) = &t {
        // check this is actually a ScriptFunction
        if let TransactionPayload::ScriptFunction(_) = &u.raw_txn.payload {
            maybe_decode_v5_genesis_function(wtx, &u.raw_txn.payload)?;
            // if still unknown TX try again with v5.2.0
            if let RelationLabel::Unknown = wtx.relation_label {
                maybe_decode_v520_function(wtx, &u.raw_txn.payload)?;
            }
        }
    }
    Ok(())
}

fn maybe_decode_v5_genesis_function(
    wtx: &mut WarehouseTxMaster,
    payload: &TransactionPayload,
) -> Result<()> {
    if let Some(sf) = &ScriptFunctionCallGenesis::decode(payload) {
        wtx.entry_function = Some(EntryFunctionArgs::V5(sf.to_owned()));
        // TODO: some script functions have very large payloads which clog the e.g. Miner. So those are only added for the catch-all txs which don't fall into categories we are interested in.
        match sf {
            ScriptFunctionCallGenesis::BalanceTransfer {
                destination,
                unscaled_value,
            } => {
                wtx.relation_label = RelationLabel::Transfer(
                    cast_legacy_account(destination)?,
                    *unscaled_value * COIN_DECIMAL_PRECISION * LEGACY_REBASE_MULTIPLIER,
                );

                wtx.entry_function = Some(EntryFunctionArgs::V5(sf.to_owned()));
            }
            ScriptFunctionCallGenesis::AutopayCreateInstruction { .. } => {
                wtx.relation_label = RelationLabel::Configuration;
                wtx.entry_function = Some(EntryFunctionArgs::V5(sf.to_owned()));
            }
            ScriptFunctionCallGenesis::CreateAccUser { .. } => {
                // onboards self
                wtx.relation_label = RelationLabel::Onboarding(wtx.sender, 0);
            }
            ScriptFunctionCallGenesis::CreateAccVal { .. } => {
                // onboards self
                wtx.relation_label = RelationLabel::Onboarding(wtx.sender, 0);
            }

            ScriptFunctionCallGenesis::CreateUserByCoinTx {
                account,
                unscaled_value,
                ..
            } => {
                wtx.relation_label = RelationLabel::Onboarding(
                    cast_legacy_account(account)?,
                    *unscaled_value * COIN_DECIMAL_PRECISION * LEGACY_REBASE_MULTIPLIER,
                );
            }
            ScriptFunctionCallGenesis::CreateValidatorAccount {
                sliding_nonce: _,
                new_account_address,
                ..
            } => {
                wtx.relation_label =
                    RelationLabel::Onboarding(cast_legacy_account(new_account_address)?, 0);
            }
            ScriptFunctionCallGenesis::CreateValidatorOperatorAccount {
                sliding_nonce: _,
                new_account_address,
                ..
            } => {
                wtx.relation_label =
                    RelationLabel::Onboarding(cast_legacy_account(new_account_address)?, 0);
            }

            ScriptFunctionCallGenesis::MinerstateCommit { .. } => {
                wtx.relation_label = RelationLabel::Miner;
            }
            ScriptFunctionCallGenesis::MinerstateCommitByOperator { .. } => {
                wtx.relation_label = RelationLabel::Miner;
            }
            _ => {
                wtx.relation_label = RelationLabel::Unknown;

                wtx.entry_function = Some(EntryFunctionArgs::V5(sf.to_owned()));
            }
        }
    }
    Ok(())
}

fn maybe_decode_v520_function(
    wtx: &mut WarehouseTxMaster,
    payload: &TransactionPayload,
) -> Result<()> {
    if let Some(sf) = &ScriptFunctionCallV520::decode(payload) {
        wtx.entry_function = Some(EntryFunctionArgs::V520(sf.to_owned()));
        match sf {
            // NOTE: This balanceTransfer likely de/encodes to the same
            // bytes as v5 genesis
            ScriptFunctionCallV520::BalanceTransfer {
                destination,
                unscaled_value,
            } => {
                wtx.relation_label = RelationLabel::Transfer(
                    cast_legacy_account(destination)?,
                    *unscaled_value * COIN_DECIMAL_PRECISION * LEGACY_REBASE_MULTIPLIER,
                );

                wtx.entry_function = Some(EntryFunctionArgs::V520(sf.to_owned()));
            }
            ScriptFunctionCallV520::CreateAccUser { .. } => {
                wtx.relation_label = RelationLabel::Onboarding(wtx.sender, 0);
            }
            ScriptFunctionCallV520::CreateAccVal { .. } => {
                wtx.relation_label = RelationLabel::Onboarding(wtx.sender, 0);
            }

            ScriptFunctionCallV520::CreateValidatorAccount {
                sliding_nonce: _,
                new_account_address,
                ..
            } => {
                wtx.relation_label =
                    RelationLabel::Onboarding(cast_legacy_account(new_account_address)?, 0);
            }
            ScriptFunctionCallV520::CreateValidatorOperatorAccount {
                sliding_nonce: _,
                new_account_address,
                ..
            } => {
                wtx.relation_label =
                    RelationLabel::Onboarding(cast_legacy_account(new_account_address)?, 0);
            }
            ScriptFunctionCallV520::MinerstateCommit { .. } => {
                wtx.relation_label = RelationLabel::Miner;
            }
            ScriptFunctionCallV520::MinerstateCommitByOperator { .. } => {
                wtx.relation_label = RelationLabel::Miner;
            }
            _ => {
                wtx.relation_label = RelationLabel::Unknown;
                wtx.entry_function = Some(EntryFunctionArgs::V520(sf.to_owned()));
            }
        }
    }
    Ok(())
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

/// gets all json files decompressed from tgz
pub fn list_all_tgz_archives(search_dir: &Path) -> Result<Vec<PathBuf>> {
    let path = search_dir.canonicalize()?;

    let pattern = format!(
        "{}/**/*.tgz",
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

fn cast_legacy_account(legacy: &LegacyAddressV5) -> Result<AccountAddress> {
    Ok(AccountAddress::from_hex_literal(&legacy.to_hex_literal())?)
}

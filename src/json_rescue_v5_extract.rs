use crate::{
    schema_transaction::{EntryFunctionArgs, RelationLabel, WarehouseEvent, WarehouseTxMaster},
    unzip_temp::decompress_tar_archive,
};
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
                wtxs.sender = cast_legacy_account(sender)?;

                // must cast from V5 Hashvalue buffer layout
                wtxs.tx_hash = HashValue::from_slice(&t.hash.to_vec())?;

                wtxs.function = make_function_name(script);
                trace!("function: {}", &wtxs.function);

                decode_transaction_args(&mut wtxs, &t.bytes)?;

                // TODO:
                // wtxs.events
                // wtxs.block_timestamp

                // TODO: create arg to exclude miner txs
                if wtxs.relation_label != RelationLabel::Miner {
                    tx_vec.push(wtxs);
                }
            }
            TransactionDataView::BlockMetadata { timestamp_usecs: _ } => {
                // TODO get epoch events
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

pub fn decode_transaction_args(wtx: &mut WarehouseTxMaster, tx_bytes: &[u8]) -> Result<()> {
    // test we can bcs decode to the transaction object
    let t: TransactionV5 = bcs::from_bytes(tx_bytes).unwrap();

    if let TransactionV5::UserTransaction(u) = &t {
        if let TransactionPayload::ScriptFunction(_) = &u.raw_txn.payload {
            if let Some(sf) = &ScriptFunctionCallGenesis::decode(&u.raw_txn.payload) {
                // TODO: some script functions have very large payloads which clog the e.g. Miner. So those are only added for the catch-all txs which don't fall into categories we are interested in.
                match sf {
                    ScriptFunctionCallGenesis::BalanceTransfer { destination, .. } => {
                        wtx.relation_label =
                            RelationLabel::Transfer(cast_legacy_account(destination)?);

                        wtx.entry_function = Some(EntryFunctionArgs::V5(sf.to_owned()));
                    }
                    ScriptFunctionCallGenesis::CreateAccUser { .. } => {
                        // onboards self
                        wtx.relation_label = RelationLabel::Onboarding(wtx.sender);
                    }
                    ScriptFunctionCallGenesis::CreateAccVal { .. } => {
                        // onboards self
                        wtx.relation_label = RelationLabel::Onboarding(wtx.sender);
                    }

                    ScriptFunctionCallGenesis::CreateUserByCoinTx { account, .. } => {
                        wtx.relation_label =
                            RelationLabel::Onboarding(cast_legacy_account(account)?);
                    }
                    ScriptFunctionCallGenesis::CreateValidatorAccount {
                        sliding_nonce: _,
                        new_account_address,
                        ..
                    } => {
                        wtx.relation_label =
                            RelationLabel::Onboarding(cast_legacy_account(new_account_address)?);
                    }
                    ScriptFunctionCallGenesis::CreateValidatorOperatorAccount {
                        sliding_nonce: _,
                        new_account_address,
                        ..
                    } => {
                        wtx.relation_label =
                            RelationLabel::Onboarding(cast_legacy_account(new_account_address)?);
                    }

                    ScriptFunctionCallGenesis::MinerstateCommit { .. } => {
                        wtx.relation_label = RelationLabel::Miner;
                    }
                    ScriptFunctionCallGenesis::MinerstateCommitByOperator { .. } => {
                        wtx.relation_label = RelationLabel::Miner;
                    }
                    _ => {
                        wtx.relation_label = RelationLabel::Configuration;

                        wtx.entry_function = Some(EntryFunctionArgs::V5(sf.to_owned()));
                    }
                }
            }

            if let Some(sf) = &ScriptFunctionCallV520::decode(&u.raw_txn.payload) {
                match sf {
                    ScriptFunctionCallV520::CreateAccUser { .. } => {
                        wtx.relation_label = RelationLabel::Onboarding(wtx.sender);
                    }
                    ScriptFunctionCallV520::CreateAccVal { .. } => {
                        wtx.relation_label = RelationLabel::Onboarding(wtx.sender);
                    }

                    ScriptFunctionCallV520::CreateValidatorAccount {
                        sliding_nonce: _,
                        new_account_address,
                        ..
                    } => {
                        wtx.relation_label =
                            RelationLabel::Onboarding(cast_legacy_account(new_account_address)?);
                    }
                    ScriptFunctionCallV520::CreateValidatorOperatorAccount {
                        sliding_nonce: _,
                        new_account_address,
                        ..
                    } => {
                        wtx.relation_label =
                            RelationLabel::Onboarding(cast_legacy_account(new_account_address)?);
                    }
                    ScriptFunctionCallV520::MinerstateCommit { .. } => {
                        wtx.relation_label = RelationLabel::Miner;
                    }
                    ScriptFunctionCallV520::MinerstateCommitByOperator { .. } => {
                        wtx.relation_label = RelationLabel::Miner;
                    }
                    _ => {
                        wtx.relation_label = RelationLabel::Configuration;
                        wtx.entry_function = Some(EntryFunctionArgs::V520(sf.to_owned()));
                    }
                }
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

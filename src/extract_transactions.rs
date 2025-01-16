use crate::decode_entry_function::decode_entry_function_all_versions;
use crate::schema_transaction::{RelationLabel, UserEventTypes, WarehouseEvent, WarehouseTxMaster};
use anyhow::Result;
use chrono::DateTime;
use diem_crypto::HashValue;
use diem_types::account_config::{NewBlockEvent, WithdrawEvent};
use diem_types::contract_event::ContractEvent;
use diem_types::{account_config::DepositEvent, transaction::SignedTransaction};
use glob::glob;
use libra_storage::read_tx_chunk::{load_chunk, load_tx_chunk_manifest};
use libra_types::move_resource::coin_register_event::CoinRegisterEvent;
use log::{error, info, warn};
use serde_json::json;
use std::path::Path;

fn maybe_fix_manifest(archive_path: &Path) -> Result<()> {
    let pattern = format!("{}/**/*.manifest", archive_path.display());
    for f in glob(&pattern)? {
        if let Some(f) = f.ok() {
            let manifest = load_tx_chunk_manifest(&manifest_file)?;
            manifest.chunks.iter_mut().map(|e| {
                if e.proof.contains(".gz") {
                    e.proof = *e.proof.trim_end_matches(".gz")
                }
            });
            let literal = serde_json::to_string(&manifest)?;
            std::fs::write(manifest_file, literal.as_bytes());
            warn!(
                "rewriting .manifest file to remove .gz paths, {}",
                archive_path.display()
            )
        }
    }
    Ok(())
}
pub fn maybe_handle_gz(archive_path: &Path) -> Result<(PathBuf, Option<TempPath>)> {
    let pattern = format!("{}/*.*.gz", archive_path.display());
    if !glob(&pattern)?.is_empty() {
        let (p, tp) = make_temp_unzipped(f, false);
        maybe_fix_manifest(archive_path);
        return Ok((p, Some(tp)));
    }
    // check if the files are .gz
    // check if files are unzipped, skip next step
    let pattern = format!("{}/**/*.proof", archive_path.display());
    assert!(
        !glob(&pattern)?.is_empty(),
        "doesn't seem to be an decompressed archived"
    );
    // check if manifest file incorrectly has the .gz handle fix that.
    // try to load it
    let manifest = load_tx_chunk_manifest(&manifest_file)?;
    maybe_fix_manifest(archive_path);

    Ok((archive_path, None))
}

pub async fn extract_current_transactions(
    archive_path: &Path,
) -> Result<(Vec<WarehouseTxMaster>, Vec<WarehouseEvent>)> {
    let manifest_file = archive_path.join("transaction.manifest");
    assert!(
        manifest_file.exists(),
        "{}",
        &format!("transaction.manifest file not found at {:?}", archive_path)
    );
    let manifest = load_tx_chunk_manifest(&manifest_file)?;

    let mut user_txs_in_chunk = 0;
    let mut epoch = 0;
    let mut round = 0;
    let mut timestamp = 0;

    let mut user_txs: Vec<WarehouseTxMaster> = vec![];
    let mut events: Vec<WarehouseEvent> = vec![];

    for each_chunk_manifest in manifest.chunks {
        let chunk = load_chunk(archive_path, each_chunk_manifest).await?;

        for (i, tx) in chunk.txns.iter().enumerate() {
            // TODO: unsure if this is off by one
            // perhaps reverse the vectors before transforming

            // first increment the block metadata. This assumes the vector is sequential.
            if let Some(block) = tx.try_as_block_metadata() {
                // check the epochs are incrementing or not
                if epoch > block.epoch()
                    && round > block.round()
                    && timestamp > block.timestamp_usecs()
                {
                    dbg!(
                        epoch,
                        block.epoch(),
                        round,
                        block.round(),
                        timestamp,
                        block.timestamp_usecs()
                    );
                }

                epoch = block.epoch();
                round = block.round();
                timestamp = block.timestamp_usecs();
            }

            let tx_info = chunk
                .txn_infos
                .get(i)
                .expect("could not index on tx_info chunk, vectors may not be same length");
            let tx_hash_info = tx_info.transaction_hash();

            let tx_events = chunk
                .event_vecs
                .get(i)
                .expect("could not index on events chunk, vectors may not be same length");

            let mut decoded_events = decode_events(tx_hash_info, tx_events)?;
            events.append(&mut decoded_events);

            if let Some(signed_transaction) = tx.try_as_signed_user_txn() {
                let tx =
                    make_master_tx(signed_transaction, epoch, round, timestamp, decoded_events)?;

                // sanity check that we are talking about the same block, and reading vectors sequentially.
                if tx.tx_hash != tx_hash_info {
                    error!("transaction hashes do not match in transaction vector and transaction_info vector");
                }

                if tx.relation_label.get_recipient().is_some() {
                    user_txs.push(tx);
                    user_txs_in_chunk += 1;
                }
            }
        }
        info!("user transactions found in chunk: {}", chunk.txns.len());
        info!("user transactions extracted: {}", user_txs.len());
        if user_txs_in_chunk != user_txs.len() {
            warn!("some transactions excluded from extraction");
        }
    }

    Ok((user_txs, events))
}

pub fn make_master_tx(
    user_tx: &SignedTransaction,
    epoch: u64,
    round: u64,
    block_timestamp: u64,
    events: Vec<WarehouseEvent>,
) -> Result<WarehouseTxMaster> {
    let tx_hash = user_tx.clone().committed_hash();
    let raw = user_tx.raw_transaction_ref();
    let p = raw.clone().into_payload().clone();
    let function = match p {
        diem_types::transaction::TransactionPayload::Script(_script) => "Script".to_owned(),
        diem_types::transaction::TransactionPayload::ModuleBundle(_module_bundle) => {
            "ModuleBundle".to_owned()
        }
        diem_types::transaction::TransactionPayload::EntryFunction(ef) => {
            format!("{}::{}", ef.module().short_str_lossless(), ef.function())
        }
        diem_types::transaction::TransactionPayload::Multisig(_multisig) => "Multisig".to_string(),
    };
    let (ef_args_opt, relation_label) = match decode_entry_function_all_versions(user_tx, &events) {
        Ok((a, b)) => (Some(a), b),
        Err(_) => (None, RelationLabel::Configuration),
    };

    let tx = WarehouseTxMaster {
        tx_hash,
        expiration_timestamp: user_tx.expiration_timestamp_secs(),
        sender: user_tx.sender(),
        epoch,
        round,
        block_timestamp,
        function,
        entry_function: ef_args_opt,
        relation_label,
        block_datetime: DateTime::from_timestamp_micros(block_timestamp as i64).unwrap(),
        events,
    };

    Ok(tx)
}

pub fn decode_events(
    tx_hash: HashValue,
    tx_events: &[ContractEvent],
) -> Result<Vec<WarehouseEvent>> {
    let list: Vec<WarehouseEvent> = tx_events
        .iter()
        .filter_map(|el| {
            // exclude block announcements, too much noise
            if NewBlockEvent::try_from_bytes(el.event_data()).is_ok() {
                return None;
            }

            let event_name = el.type_tag().to_canonical_string();
            let mut event = UserEventTypes::Other;

            let mut data = json!("unknown data");

            if let Ok(e) = WithdrawEvent::try_from_bytes(el.event_data()) {
                data = json!(&e);
                event = UserEventTypes::Withdraw(e);
            }

            if let Ok(e) = DepositEvent::try_from_bytes(el.event_data()) {
                data = json!(&e);
                event = UserEventTypes::Deposit(e);
            }

            if let Ok(e) = CoinRegisterEvent::try_from_bytes(el.event_data()) {
                data = json!(&e);
                event = UserEventTypes::Onboard(e);
            }

            Some(WarehouseEvent {
                tx_hash,
                event,
                event_name,
                data,
            })
        })
        .collect();

    Ok(list)
}

// fn pick_relation(user_tx: &SignedTransaction, events: &[WarehouseEvent]) -> RelationLabel {
//     if let Some(r) = maybe_get_current_relation(user_tx, events) {
//         return r;
//     }
//     if let Some(r) = maybe_get_v7_relation(user_tx, events) {
//         return r;
//     }

//     if let Some(r) = maybe_get_v6_relation(user_tx, events) {
//         return r;
//     }

//     RelationLabel::Configuration
// }

// // Using HEAD libra-framework code base try to decode transaction
// fn maybe_get_current_relation(
//     user_tx: &SignedTransaction,
//     events: &[WarehouseEvent],
// ) -> Option<RelationLabel> {
//     let r = match EntryFunctionCall::decode(user_tx.payload()) {
//         Some(EntryFunctionCall::OlAccountTransfer { to, amount: _ }) => {
//             if is_onboarding_event(events) {
//                 RelationLabel::Onboarding(to)
//             } else {
//                 RelationLabel::Transfer(to)
//             }
//         }
//         Some(EntryFunctionCall::OlAccountCreateAccount { auth_key }) => {
//             RelationLabel::Onboarding(auth_key)
//         }
//         Some(EntryFunctionCall::VouchVouchFor { friend_account }) => {
//             RelationLabel::Vouch(friend_account)
//         }
//         Some(EntryFunctionCall::VouchInsistVouchFor { friend_account }) => {
//             RelationLabel::Vouch(friend_account)
//         }
//         Some(EntryFunctionCall::CoinTransfer { to, .. }) => RelationLabel::Transfer(to),
//         Some(EntryFunctionCall::AccountRotateAuthenticationKeyWithRotationCapability {
//             rotation_cap_offerer_address,
//             ..
//         }) => RelationLabel::Transfer(rotation_cap_offerer_address),

//         // TODO: get other entry functions with known counter parties
//         // if nothing is found try to decipher from events
//         _ => return None,
//     };
//     Some(r)
// }

// fn maybe_get_v7_relation(
//     user_tx: &SignedTransaction,
//     events: &[WarehouseEvent],
// ) -> Option<RelationLabel> {
//     let r = match V7EntryFunctionCall::decode(user_tx.payload()) {
//         Some(V7EntryFunctionCall::OlAccountTransfer { to, amount: _ }) => {
//             if is_onboarding_event(events) {
//                 RelationLabel::Onboarding(to)
//             } else {
//                 RelationLabel::Transfer(to)
//             }
//         }
//         Some(V7EntryFunctionCall::OlAccountCreateAccount { auth_key }) => {
//             RelationLabel::Onboarding(auth_key)
//         }
//         Some(V7EntryFunctionCall::VouchVouchFor { friend_account }) => {
//             RelationLabel::Vouch(friend_account)
//         }
//         Some(V7EntryFunctionCall::VouchInsistVouchFor { friend_account }) => {
//             RelationLabel::Vouch(friend_account)
//         }
//         Some(V7EntryFunctionCall::CoinTransfer { to, .. }) => RelationLabel::Transfer(to),
//         Some(V7EntryFunctionCall::AccountRotateAuthenticationKeyWithRotationCapability {
//             rotation_cap_offerer_address,
//             ..
//         }) => RelationLabel::Transfer(rotation_cap_offerer_address),

//         // TODO: get other entry functions with known counter parties
//         // if nothing is found try to decipher from events
//         _ => return None,
//     };
//     Some(r)
// }

// fn maybe_get_v6_relation(
//     user_tx: &SignedTransaction,
//     events: &[WarehouseEvent],
// ) -> Option<RelationLabel> {
//     let r = match V6EntryFunctionCall::decode(user_tx.payload()) {
//         Some(V6EntryFunctionCall::OlAccountTransfer { to, amount: _ }) => {
//             if is_onboarding_event(events) {
//                 RelationLabel::Onboarding(to)
//             } else {
//                 RelationLabel::Transfer(to)
//             }
//         }
//         Some(V6EntryFunctionCall::OlAccountCreateAccount { auth_key }) => {
//             RelationLabel::Onboarding(auth_key)
//         }
//         Some(V6EntryFunctionCall::VouchVouchFor { wanna_be_my_friend }) => {
//             RelationLabel::Vouch(wanna_be_my_friend)
//         }
//         Some(V6EntryFunctionCall::VouchInsistVouchFor { wanna_be_my_friend }) => {
//             RelationLabel::Vouch(wanna_be_my_friend)
//         }
//         Some(V6EntryFunctionCall::CoinTransfer { to, .. }) => RelationLabel::Transfer(to),
//         Some(V6EntryFunctionCall::AccountRotateAuthenticationKeyWithRotationCapability {
//             rotation_cap_offerer_address,
//             ..
//         }) => RelationLabel::Transfer(rotation_cap_offerer_address),

//         // TODO: get other entry functions with known counter parties
//         // if nothing is found try to decipher from events
//         _ => return None,
//     };
//     Some(r)
// }

// fn is_onboarding_event(events: &[WarehouseEvent]) -> bool {
//     let withdraw = events.iter().any(|e| {
//         if let UserEventTypes::Withdraw(_) = e.event {
//             return true;
//         }
//         false
//     });

//     let deposit = events.iter().any(|e| {
//         if let UserEventTypes::Deposit(_) = e.event {
//             return true;
//         }
//         false
//     });

//     let onboard = events.iter().any(|e| {
//         if let UserEventTypes::Onboard(_) = e.event {
//             return true;
//         }
//         false
//     });

//     withdraw && deposit && onboard
// }

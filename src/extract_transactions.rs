use crate::decode_entry_function::decode_entry_function_all_versions;
use crate::scan::FrameworkVersion;
use crate::schema_transaction::{RelationLabel, UserEventTypes, WarehouseEvent, WarehouseTxMaster};
use anyhow::Result;
use chrono::DateTime;
use diem_crypto::HashValue;
use diem_types::account_config::{NewBlockEvent, WithdrawEvent};
use diem_types::contract_event::ContractEvent;
use diem_types::{account_config::DepositEvent, transaction::SignedTransaction};
use libra_storage::read_tx_chunk::{load_chunk, load_tx_chunk_manifest};
use libra_types::move_resource::coin_register_event::CoinRegisterEvent;
use log::{error, info, warn};
use serde_json::json;
use std::path::Path;

pub async fn extract_current_transactions(
    archive_path: &Path,
    framework_version: &FrameworkVersion,
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
                // // check the epochs are incrementing or not
                // if epoch > block.epoch()
                //     && round > block.round()
                //     && timestamp > block.timestamp_usecs()
                // {
                //     dbg!(
                //         epoch,
                //         block.epoch(),
                //         round,
                //         block.round(),
                //         timestamp,
                //         block.timestamp_usecs()
                //     );
                // }

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
                let tx = make_master_tx(
                    signed_transaction,
                    epoch,
                    round,
                    timestamp,
                    decoded_events,
                    framework_version,
                )?;

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
    framework_version: &FrameworkVersion,
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
        framework_version: framework_version.clone(),
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

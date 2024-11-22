use std::path::Path;

use anyhow::Result;
use diem_types::account_view::AccountView;
use libra_backwards_compatibility::version_five::{
    balance_v5::BalanceResourceV5,
    ol_wallet::SlowWalletResourceV5,
    state_snapshot_v5::{v5_accounts_from_manifest_path, v5_read_from_snapshot_manifest},
};
use libra_storage::read_snapshot::{accounts_from_snapshot_backup, load_snapshot_manifest};
use libra_types::{
    exports::AccountAddress,
    move_resource::{libra_coin::LibraCoinStoreResource, wallet::SlowWalletResource},
};
use log::{error, info, warn};

use crate::{
    scan::FrameworkVersion,
    schema_account_state::{WarehouseAccState, WarehouseTime},
};

// uses libra-compatibility to parse the v5 manifest files, and decode v5 format bytecode into current version data structures (v6+);
pub async fn extract_v5_snapshot(v5_manifest_path: &Path) -> Result<Vec<WarehouseAccState>> {
    // NOTE: this is duplicated with next step.
    let manifest_data = v5_read_from_snapshot_manifest(v5_manifest_path)?;
    let account_blobs = v5_accounts_from_manifest_path(v5_manifest_path).await?;

    // TODO: see below, massively inefficient
    let time = WarehouseTime {
        framework_version: FrameworkVersion::V5,
        timestamp: 0,
        version: manifest_data.version,
        epoch: 0,
    };

    info!("account records found: {}", &account_blobs.len());
    let mut warehouse_state = vec![];
    for el in account_blobs.iter() {
        let acc = el.to_account_state()?;
        // convert v5 address to v7
        match acc.get_address() {
            Ok(a) => {
                let address_literal = a.to_hex_literal();
                let cast_address = AccountAddress::from_hex_literal(&address_literal)?;
                let mut s = WarehouseAccState::new(cast_address);

                s.time = time.clone();

                if let Ok(r) = acc.get_diem_account_resource() {
                    s.sequence_num = r.sequence_number();
                }

                if let Ok(b) = acc.get_resource::<BalanceResourceV5>() {
                    s.balance = b.coin()
                }
                if let Ok(sw) = acc.get_resource::<SlowWalletResourceV5>() {
                    s.slow_wallet_locked = sw.unlocked;
                    s.slow_wallet_transferred = sw.transferred;
                }

                warehouse_state.push(s);
            }
            Err(e) => {
                error!("could not parse blob to V5 Address: {}", &e);
            }
        }
    }

    Ok(warehouse_state)
}

pub async fn extract_current_snapshot(archive_path: &Path) -> Result<Vec<WarehouseAccState>> {
    let manifest_file = archive_path.join("state.manifest");
    assert!(
        manifest_file.exists(),
        "{}",
        &format!("state.manifest file not found at {:?}", archive_path)
    );
    let manifest = load_snapshot_manifest(&manifest_file)?;

    // TODO: this is not memory efficient, will be massively duplicating data
    // when the insert query could just use the warehouse time, for entire state
    let time = WarehouseTime {
        version: manifest.version,
        epoch: manifest.epoch,
        framework_version: FrameworkVersion::V7,
        timestamp: 0,
    };

    let accs = accounts_from_snapshot_backup(manifest, archive_path).await?;

    info!("SUCCESS: backup loaded. # accounts: {}", &accs.len());

    // TODO: stream this
    let mut warehouse_state = vec![];
    for el in accs.iter() {
        if let Some(address) = el.get_account_address()? {
            let mut s = WarehouseAccState::new(address);

            s.time = time.clone();

            if let Some(r) = el.get_account_resource()? {
                s.sequence_num = r.sequence_number();
            }

            if let Some(b) = el.get_resource::<LibraCoinStoreResource>()? {
                s.balance = b.coin();
            }

            if let Some(sw) = el.get_resource::<SlowWalletResource>()? {
                s.slow_wallet_locked = sw.unlocked;
                s.slow_wallet_transferred = sw.transferred;
            }

            warehouse_state.push(s);
        }
    }

    info!(
        "SUCCESS: accounts parsed. # accounts: {}",
        &warehouse_state.len()
    );

    if warehouse_state.len() != accs.len() {
        warn!("account count does not match");
    }

    Ok(warehouse_state)
}

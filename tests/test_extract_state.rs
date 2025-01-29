mod support;

use anyhow::Result;
use libra_forensic_db::extract_snapshot::{extract_current_snapshot, extract_v5_snapshot};
use support::fixtures::{v5_state_manifest_fixtures_path, v7_state_manifest_fixtures_path};

#[tokio::test]
async fn test_extract_v5_from_manifest() -> Result<()> {
    let archive_path = v5_state_manifest_fixtures_path();
    assert!(archive_path.exists());
    let s = extract_v5_snapshot(&archive_path).await?;
    // NOTE: the parsing drops 1 blob, which is the 0x1 account, because it would not have the DiemAccount struct on it as a user address would have.
    assert!(s.len() == 17338);
    let first = s.first().unwrap();

    assert!(&first.address.to_hex_literal() == "0x407d4d486fdc4e796504135e545be77");
    assert!(first.balance == 100135.989588);
    assert!(first.slow_wallet_unlocked == Some(140001.000000));
    assert!(first.slow_wallet_transferred == Some(15999.000000));
    assert!(first.sequence_num == 7);

    Ok(())
}

#[tokio::test]
async fn test_extract_v7_manifest() -> Result<()> {
    let archive_dir = v7_state_manifest_fixtures_path();

    let s = extract_current_snapshot(&archive_dir).await?;
    // NOTE: the parsing drops 1 blob, which is the 0x1 account, because it would not have the DiemAccount struct on it as a user address would have.
    assert!(s.len() == 24607);
    Ok(())
}

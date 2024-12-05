mod support;

use libra_forensic_db::extract_transactions::extract_current_transactions;

#[tokio::test]
async fn test_extract_tx_from_archive() -> anyhow::Result<()> {
    let archive_path = support::fixtures::v7_tx_manifest_fixtures_path();
    let list = extract_current_transactions(&archive_path).await?;

    assert!(list.0.len() == 6);

    Ok(())
}

#[tokio::test]
async fn test_extract_v6_tx_from_archive() -> anyhow::Result<()> {
    let archive_path = support::fixtures::v6_tx_manifest_fixtures_path();
    let list = extract_current_transactions(&archive_path).await?;

    assert!(list.0.len() == 27);
    assert!(list.1.len() == 52);

    Ok(())
}

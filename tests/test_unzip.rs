mod support;
use libra_forensic_db::unzip_temp;
use libra_storage::read_tx_chunk::load_tx_chunk_manifest;

#[ignore]
#[test]
fn test_unzip() {
    let archive_path = support::fixtures::v7_tx_manifest_fixtures_path();
    let (_, temp_unzipped_dir) =
        unzip_temp::test_helper_temp_unzipped(&archive_path, false).unwrap();

    assert!(temp_unzipped_dir.path().exists());
    assert!(temp_unzipped_dir
        .path()
        .join("transaction.manifest")
        .exists())
}

#[tokio::test]
async fn test_extract_tx_with_gz_bug_from_archive() -> anyhow::Result<()> {
    let fixture_path = support::fixtures::v7_tx_manifest_fixtures_path();
    let fixture_path = fixture_path.parent().unwrap();

    let (archive_path, temppath_opt) =
        unzip_temp::maybe_handle_gz(&fixture_path.join("transaction_95700001-.46cf"))?;

    let temp_unzipped = temppath_opt.unwrap();
    assert!(temp_unzipped.path().exists());

    let manifest = load_tx_chunk_manifest(&archive_path.join("transaction.manifest"))?;

    let chunk_path = temp_unzipped.path().join(&manifest.chunks[0].transactions);

    assert!(chunk_path.exists());

    Ok(())
}

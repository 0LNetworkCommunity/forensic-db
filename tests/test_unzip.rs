mod support;
use libra_forensic_db::unzip_temp;

#[ignore]
#[test]
fn test_unzip() {
    let archive_path = support::fixtures::v7_tx_manifest_fixtures_path();
    let (_, temp_unzipped_dir) = unzip_temp::make_temp_unzipped(&archive_path, false).unwrap();

    assert!(temp_unzipped_dir.path().exists());
    assert!(temp_unzipped_dir
        .path()
        .join("transaction.manifest")
        .exists())
}

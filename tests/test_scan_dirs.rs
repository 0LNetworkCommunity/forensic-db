mod support;

use anyhow::Result;
use libra_forensic_db::scan::{scan_dir_archive, BundleContent, FrameworkVersion};
use support::fixtures;

#[test]

fn test_scan_dir_for_v5_manifests() -> Result<()> {
    let start_here = fixtures::v5_fixtures_path();

    let s = scan_dir_archive(&start_here, None)?;

    assert!(s.0.len() == 1);
    let (_k, v) = s.0.first_key_value().unwrap();
    assert!(v.version == FrameworkVersion::V5);
    assert!(v.contents == BundleContent::StateSnapshot);

    Ok(())
}

#[test]
fn test_scan_dir_for_v7_manifests() -> Result<()> {
    let start_here = fixtures::v7_fixtures_path();

    let s = scan_dir_archive(&start_here, None)?;

    let archives = s.0;
    assert!(archives.len() == 5);

    Ok(())
}

// TODO: check scan dirs
#[ignore]
#[test]
fn test_scan_dir_for_compressed_v7_manifests() -> Result<()> {
    let start_here = fixtures::v7_fixtures_gzipped();

    let _archives = scan_dir_archive(&start_here, None)?;

    // a normal scan should find no files.
    // assert!(archives.0.iter().len() == 0);

    // // This time the scan should find readable files
    // let (_, unzipped_dir) = test_helper_temp_unzipped(&start_here, false)?;

    // let archives = scan_dir_archive(unzipped_dir.path(), None)?;
    // assert!(archives.0.iter().len() > 0);

    Ok(())
}

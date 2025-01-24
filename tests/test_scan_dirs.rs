mod support;

use anyhow::Result;
use libra_forensic_db::{
    scan::{scan_dir_archive, BundleContent, FrameworkVersion},
    unzip_temp::make_temp_unzipped,
};
use support::fixtures;

#[test]

fn test_scan_dir_for_v5_manifests() -> Result<()> {
    let start_here = fixtures::v5_fixtures_path();

    let s = scan_dir_archive(&start_here, None)?;

    dbg!(&s);
    assert!(s.0.len() == 1);
    let (_k, v) = s.0.first_key_value().unwrap();
    assert!(v.version == FrameworkVersion::V5);
    assert!(v.contents == BundleContent::StateSnapshot);

    Ok(())
}
// #[test]

// fn test_scan_dir_for_v5_final() -> Result<()> {
//     let start_here = fixtures::v5_fixtures_path();
//     let s = start_here.parent().unwrap().join("v5_final_epoch/state_ver_141722729.0ab2");
//     let s = scan_dir_archive(&s, None)?;

//     dbg!(&s);

//     assert!(s.0.len() == 1);
//     Ok(())
// }

#[test]
fn test_scan_dir_for_v7_manifests() -> Result<()> {
    let start_here = fixtures::v7_fixtures_path();

    let s = scan_dir_archive(&start_here, None)?;

    let archives = s.0;
    assert!(archives.len() == 3);

    Ok(())
}

#[ignore]
#[test]
fn test_scan_dir_for_compressed_v7_manifests() -> Result<()> {
    let start_here = fixtures::v7_fixtures_gzipped();

    let archives = scan_dir_archive(&start_here, None)?;

    // a normal scan should find no files.
    assert!(archives.0.iter().len() == 0);

    // This time the scan should find readable files
    let (_, unzipped_dir) = make_temp_unzipped(&start_here, false)?;

    let archives = scan_dir_archive(unzipped_dir.path(), None)?;
    assert!(archives.0.iter().len() > 0);

    Ok(())
}

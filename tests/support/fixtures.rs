use std::path::PathBuf;

pub fn v5_fixtures_path() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.join("tests/fixtures/v5")
}

pub fn v7_fixtures_path() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.join("tests/fixtures/v7")
}

pub fn v7_fixtures_gzipped() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.join("tests/fixtures/v7/transaction_38100001-.541f_gzipped")
}

pub fn v5_json_tx_path() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.join("tests/fixtures/v5/json-rescue")
}

pub fn v5_state_manifest_fixtures_path() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dir = p.join("tests/fixtures/v5/state_ver_119757649.17a8");
    assert!(
        &dir.exists(),
        "fixtures for backup archive cannot be found at path {}",
        &dir.display()
    );

    dir.to_owned()
}

pub fn v7_state_manifest_fixtures_path() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .unwrap();
    assert!(&p.exists(), "cannot find project root dir");
    let dir = p.join("tests/fixtures/v7/state_epoch_116_ver_38180075.05af");
    assert!(
        &dir.exists(),
        "fixtures for backup archive cannot be found at path {}",
        &dir.display()
    );
    dir
}

pub fn v7_tx_manifest_fixtures_path() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .unwrap();
    assert!(&p.exists(), "cannot find project root dir");
    let dir = p.join("tests/fixtures/v7/transaction_38100001-.541f");
    assert!(
        &dir.exists(),
        "fixtures for backup archive cannot be found at path {}",
        &dir.display()
    );
    dir
}

pub fn v6_tx_manifest_fixtures_path() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .unwrap();
    assert!(&p.exists(), "not at the cargo manifest dir");
    let dir = p.join("tests/fixtures/v6/transaction_9900001-.e469");
    assert!(
        &dir.exists(),
        "fixtures for backup archive cannot be found at path {}",
        &dir.display()
    );
    dir
}

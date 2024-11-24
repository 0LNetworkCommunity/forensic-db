mod support;

use libra_forensic_db::{
    json_rescue_v5_compat::TransactionViewV5,
    json_rescue_v5_extract::{decompress_to_temppath, extract_v5_json_rescue},
};
use support::fixtures;

#[test]
fn test_rescue_v5_parse() -> anyhow::Result<()> {
    let path = fixtures::v5_json_tx_path().join("example_user_tx.json");
    let json = std::fs::read_to_string(path)?;

    let txs: Vec<TransactionViewV5> = serde_json::from_str(&json)?;

    let first = txs.first().unwrap();
    assert!(first.gas_used == 1429);

    Ok(())
}

#[test]
fn test_json_format_example() -> anyhow::Result<()> {
    let p = fixtures::v5_json_tx_path().join("example_user_tx.json");

    let (tx, _) = extract_v5_json_rescue(&p)?;
    let first = tx.first().unwrap();
    dbg!(&tx);

    assert!(first.sender.to_hex_literal() == "0xc8336044cdf1878d9738ed0a041b235e");
    Ok(())
}

#[test]
fn test_json_full_file() -> anyhow::Result<()> {
    let p = fixtures::v5_json_tx_path().join("0-999.json");

    let (tx, _) = extract_v5_json_rescue(&p)?;
    dbg!(&tx.len());

    Ok(())
}

#[test]
fn decompress_and_read() {
    let path = fixtures::v5_json_tx_path().join("0-99900.tgz");

    let temp_dir = decompress_to_temppath(&path).unwrap();

    let first_file = temp_dir.path().join("0-999.json");
    let (tx, _) = extract_v5_json_rescue(&first_file).unwrap();
    dbg!(&tx.len());
    assert!(tx.len() == 11);
    let first = tx.first().unwrap();

    assert!(first.sender.to_hex_literal() == "0xc8336044cdf1878d9738ed0a041b235e");
}

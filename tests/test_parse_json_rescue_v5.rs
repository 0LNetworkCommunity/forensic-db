mod support;
use libra_forensic_db::{
    json_rescue_v5_compat::TransactionViewV5, json_rescue_v5_extract::extract_v5_json_rescue,
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
fn test_extract_v5_json_from_file() -> anyhow::Result<()> {
    let p = fixtures::v5_json_tx_path().join("example_user_tx.json");

    let (tx, _) = extract_v5_json_rescue(&p)?;
    let first = tx.first().unwrap();
    dbg!(&tx);

    assert!(first.sender.to_hex_literal() == "0xc8336044cdf1878d9738ed0a041b235e");
    Ok(())
}

mod support;
use libra_forensic_db::json_rescue_v5_parse::TransactionView;
use support::fixtures;

#[test]
fn test_rescue_v5_parse() -> anyhow::Result<()> {
    let path = fixtures::v5_json_tx_path().join("example_user_tx.json");
    let json = std::fs::read_to_string(path)?;

    let txs: Vec<TransactionView> = serde_json::from_str(&json)?;

    let first = txs.first().unwrap();
    assert!(first.gas_used == 1429);

    Ok(())
}

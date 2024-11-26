mod support;

use libra_backwards_compatibility::{
    sdk::v5_0_0_genesis_transaction_script_builder::ScriptFunctionCall,
    version_five::{
        transaction_type_v5::{TransactionPayload, TransactionV5},
        transaction_view_v5::TransactionViewV5,
    },
};
use libra_forensic_db::json_rescue_v5_extract::{decompress_to_temppath, extract_v5_json_rescue};
use support::fixtures;

#[test]
fn test_rescue_v5_genesis_create_account() -> anyhow::Result<()> {
    libra_forensic_db::log_setup();

    let path = fixtures::v5_json_tx_path().join("example_create_user.json");
    let json = std::fs::read_to_string(path)?;

    let txs: Vec<TransactionViewV5> = serde_json::from_str(&json)?;

    let first = txs.first().unwrap();

    // test we can bcs decode to the transaction object
    let t: TransactionV5 = bcs::from_bytes(&first.bytes).unwrap();

    if let TransactionV5::UserTransaction(u) = &t {
        if let TransactionPayload::ScriptFunction(script_function) = &u.raw_txn.payload {
            assert!(script_function.function().as_str() == "create_user_by_coin_tx");

            let sf = ScriptFunctionCall::decode(&u.raw_txn.payload).unwrap();
            if let ScriptFunctionCall::CreateUserByCoinTx { account, .. } = sf {
                assert!(&account.to_string().to_uppercase() == "F605FE7F787551EEA808EE9ACDB98897");
            }
        }
    }

    Ok(())
}

#[test]
fn test_rescue_v5_parse_miner_tx() -> anyhow::Result<()> {
    let path = fixtures::v5_json_tx_path().join("example_miner_tx.json");
    let json = std::fs::read_to_string(path)?;

    let txs: Vec<TransactionViewV5> = serde_json::from_str(&json)?;

    let first = txs.first().unwrap();
    assert!(first.gas_used == 1429);

    // test we can bcs decode to the transaction object
    let t: TransactionV5 = bcs::from_bytes(&first.bytes).unwrap();

    if let TransactionV5::UserTransaction(u) = &t {
        if let TransactionPayload::ScriptFunction(_) = &u.raw_txn.payload {
            println!("ScriptFunction");
            dbg!(&u.raw_txn.payload);
            let sf = ScriptFunctionCall::decode(&u.raw_txn.payload);
            dbg!(&sf);
        }
    }

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
    libra_forensic_db::log_setup();
    let p = fixtures::v5_json_tx_path().join("0-999.json");

    let (tx, _) = extract_v5_json_rescue(&p)?;

    let first = tx.first().unwrap();
    dbg!(&first.entry_function);

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

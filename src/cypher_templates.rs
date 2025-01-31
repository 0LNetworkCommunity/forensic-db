//! organic free trade template literals for cypher queries
use anyhow::{Context, Result};

// batch tx submission query
// takes a Cypher Map object `list_str`
// and unwinds it into multiple merge operations
pub fn write_batch_tx_string(list_str: &str) -> String {
    format!(
        r#"
WITH {list_str} AS tx_data
UNWIND tx_data AS tx

// NOTE: users should have already been merged in a previous call
MERGE (from:Account {{address: tx.sender}})
MERGE (to:Account {{address: tx.recipient}})
MERGE (from)-[rel:Tx {{tx_hash: tx.tx_hash}}]->(to)

ON CREATE SET rel.cypher_created_at = timestamp(), rel.cypher_modified_at = null
ON MATCH SET rel.cypher_modified_at = timestamp()
SET
    rel.block_datetime = tx.block_datetime,
    rel.block_timestamp = tx.block_timestamp,
    rel.relation = tx.relation,
    rel.function = tx.function,
    rel.framework_version = tx.framework_version

// Conditionally add `tx.args` if it exists
FOREACH (_ IN CASE WHEN tx.args IS NOT NULL THEN [1] ELSE [] END |
    SET rel += tx.args
)

// Conditionally increment the lifetime coins sent
FOREACH (_ IN CASE WHEN tx.coins > 0 THEN [1] ELSE [] END |
    SET rel.coins = tx.coins
    MERGE (from)-[relTotal:Lifetime]->(to)
    SET relTotal.coins = COALESCE(relTotal.coins, 0) + tx.coins
)

RETURN
  COUNT(CASE WHEN rel.cypher_created_at = timestamp() THEN 1 END) AS created_tx,
  COUNT(CASE WHEN rel.cypher_modified_at = timestamp() AND rel.created_at < timestamp() THEN 1 END) AS modified_tx
"#
    )
}

// // TODO move this to a .CQL file so we can lint and debug
// pub fn write_batch_tx_string(list_str: &str) -> String {
//     format!(
//         r#"
// WITH {list_str} AS tx_data
// UNWIND tx_data AS tx

// // Ensure accounts exist
// MERGE (from:Account {{address: tx.sender}})
// MERGE (to:Account {{address: tx.recipient}})

// // Dynamically set the relationship label using a subquery
// WITH from, to, tx
// CALL {{
//     // Conditionally create the appropriate relationship
//     FOREACH (_ IN CASE WHEN tx.relation = "Transfer" THEN [1] ELSE [] END |
//         MERGE (from)-[rel:Transfer {tx_hash: tx.tx_hash}]->(to)
//         ON CREATE SET
//         rel.cypher_created_at = timestamp(),
//         rel.cypher_modified_at = null
//     ON MATCH SET
//         rel.cypher_modified_at = timestamp()
//     SET
//         rel.block_datetime = tx.block_datetime,
//         rel.block_timestamp = tx.block_timestamp,
//         rel.function = tx.function
//     )
//     FOREACH (_ IN CASE WHEN tx.relation = "Onboarding" THEN [1] ELSE [] END |
//         MERGE (from)-[rel:Onboarding {tx_hash: tx.tx_hash}]->(to)
//         ON CREATE SET
//         rel.cypher_created_at = timestamp(),
//         rel.cypher_modified_at = null
//     ON MATCH SET
//         rel.cypher_modified_at = timestamp()
//     SET
//         rel.block_datetime = tx.block_datetime,
//         rel.block_timestamp = tx.block_timestamp,
//         rel.function = tx.function
//     )
//     FOREACH (_ IN CASE WHEN tx.relation = "Vouch" THEN [1] ELSE [] END |
//         MERGE (from)-[rel:Vouch {tx_hash: tx.tx_hash}]->(to)
//         ON CREATE SET
//         rel.cypher_created_at = timestamp(),
//         rel.cypher_modified_at = null
//     ON MATCH SET
//         rel.cypher_modified_at = timestamp()
//     SET
//         rel.block_datetime = tx.block_datetime,
//         rel.block_timestamp = tx.block_timestamp,
//         rel.function = tx.function
//     )
//     FOREACH (_ IN CASE WHEN tx.relation IS NULL OR NOT tx.relation IN ["Transfer", "Onboarding", "Vouch"] THEN [1] ELSE [] END |
//         MERGE (from)-[rel:Misc {tx_hash: tx.tx_hash}]->(to)
//         ON CREATE SET
//         rel.cypher_created_at = timestamp(),
//         rel.cypher_modified_at = null
//     ON MATCH SET
//         rel.cypher_modified_at = timestamp()
//     SET
//         rel.block_datetime = tx.block_datetime,
//         rel.block_timestamp = tx.block_timestamp,
//         rel.function = tx.function
//         CASE
//           WHEN tx.args IS NOT NULL THEN
//             SET rel += tx.args
//         END

//     )
// }}

// // // Conditionally add `tx.args` if it exists
// // FOREACH (_ IN CASE WHEN tx.args IS NOT NULL THEN [1] ELSE [] END |
// //     SET rel += tx.args
// // )

// // Increment the cumulative Lifetime edge if `tx.amount > 0`
// FOREACH (_ IN CASE WHEN tx.amount > 0 THEN [1] ELSE [] END |
//     MERGE (from)-[rl:Lifetime]->(to)
//     SET rl.coins_tx = COALESCE(rl.amount, 0) + tx.amount
// )

// // Final return with counts
// RETURN
//   COUNT(CASE WHEN rel.cypher_created_at = timestamp() THEN 1 END) AS created_tx,
//   COUNT(CASE WHEN rel.cypher_modified_at = timestamp() AND rel.created_at < timestamp() THEN 1 END) AS modified_tx
// "#
//     )
// }

// // TODO move this to a .CQL file so we can lint and debug
// pub fn write_batch_tx_string(list_str: &str) -> String {
//     format!(
//         r#"
// WITH {list_str} AS tx_data
// UNWIND tx_data AS tx

// // Ensure accounts exist
// MERGE (from:Account {{address: tx.sender}})
// MERGE (to:Account {{address: tx.recipient}})

// // Dynamically set the relationship label using a subquery
// WITH from, to, tx
// CALL {{
//     WITH tx
//     RETURN CASE
//         WHEN tx.relation = "Tx" THEN "Tx"
//         WHEN tx.relation = "Onboarding" THEN "Vouch"
//         WHEN tx.relation = "Vouch" THEN "Vouch"
//         ELSE "Unknown" // Default for unexpected or missing values
//     END AS dynamicLabel
// }}
// WITH from, to, tx, dynamicLabel
// // Use dynamicLabel to create the relationship
// MERGE (from)-[rel:`${{dynamicLabel}}` {{tx_hash: tx.tx_hash}}]->(to)
// ON CREATE SET
//     rel.cypher_created_at = timestamp(),
//     rel.cypher_modified_at = null
// ON MATCH SET
//     rel.cypher_modified_at = timestamp()
// SET
//     rel.block_datetime = tx.block_datetime,
//     rel.block_timestamp = tx.block_timestamp,
//     rel.function = tx.function

// // Conditionally add `tx.args` if it exists
// FOREACH (_ IN CASE WHEN tx.args IS NOT NULL THEN [1] ELSE [] END |
//     SET rel += tx.args
// )

// // Increment the cumulative Lifetime edge if `tx.amount > 0`
// FOREACH (_ IN CASE WHEN tx.amount > 0 THEN [1] ELSE [] END |
//     MERGE (from)-[rl:Lifetime]->(to)
//     SET rl.coins_tx = COALESCE(rl.amount, 0) + tx.amount
// )

// // Final return with counts
// RETURN
//   COUNT(CASE WHEN rel.cypher_created_at = timestamp() THEN 1 END) AS created_tx,
//   COUNT(CASE WHEN rel.cypher_modified_at = timestamp() AND rel.created_at < timestamp() THEN 1 END) AS modified_tx
// "#
//     )
// }

pub fn write_batch_user_create(list_str: &str) -> String {
    format!(
        r#"
WITH {list_str} AS tx_data
UNWIND tx_data AS tx
WITH COLLECT(DISTINCT tx.sender) + COLLECT(DISTINCT tx.recipient) AS unique_addresses
// Deduplicate the combined list to ensure only unique addresses
UNWIND unique_addresses AS each_addr
WITH COLLECT(DISTINCT each_addr) as unique_array

UNWIND unique_array AS addr
// Merge unique Accounts
MERGE (node:Account {{address: addr}})
ON CREATE SET
    node.cypher_created_at = timestamp(),
    node.cypher_modified_at = null
ON MATCH SET
    node.cypher_modified_at = timestamp()

RETURN
  COUNT(node) AS unique_accounts,
  COUNT(CASE WHEN node.cypher_created_at = timestamp() THEN 1 END) AS created_accounts,
  COUNT(CASE WHEN node.cypher_modified_at = timestamp() AND node.cypher_created_at < timestamp() THEN 1 END) AS modified_accounts,
  COUNT(CASE WHEN node.cypher_modified_at < timestamp() THEN 1 END) AS unchanged_accounts
"#
    )
}

use log::warn;
use serde::Serialize;
use serde_json::Value;

/// Converts a serializable struct to a Cypher-compatible object string,
/// handling nested objects, arrays, and basic types.
///
/// # Arguments
/// - `object`: The serializable struct.
///
/// # Returns
/// A string in the format `{key: value, nested: {key2: value2}, array: [value3, value4]}` that can be used in Cypher queries.
///  Thanks Copilot ;)
pub fn to_cypher_object<T: Serialize>(object: &T) -> Result<String> {
    // Serialize the struct to a JSON value
    let serialized_value = serde_json::to_value(object).expect("Failed to serialize");

    let flattener = smooth_json::Flattener {
        separator: "_",
        ..Default::default()
    };

    // Convert the JSON value into a map for easy processing
    let flat = flattener.flatten(&serialized_value);
    let map = flat.as_object().context("cannot map on json object")?;
    // Build properties part of the Cypher object
    let properties: Vec<String> = map
        .into_iter()
        .map(|(key, value)| {
            let formatted_value = match value {
                Value::String(s) => format!("'{}'", s), // Wrap strings in single quotes
                Value::Number(n) => n.to_string(),      // Use numbers as-is
                Value::Bool(b) => b.to_string(),        // Use booleans as-is
                Value::Null => "null".to_string(),      // Represent null values
                Value::Array(arr) => {
                    // Handle arrays by formatting each element
                    let elements: Vec<String> = arr
                        .iter()
                        .map(|elem| match elem {
                            Value::String(s) => format!("'{}'", s),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => "null".to_string(),
                            Value::Object(_) => {
                                to_cypher_object(elem).unwrap_or("error".to_owned())
                            } // Recurse for nested objects in arrays
                            _ => "Unsupported type in array for Cypher serialization".to_string(),
                        })
                        .collect();
                    format!("[{}]", elements.join(", "))
                }
                Value::Object(_) => {
                    warn!("the json should have been flattened before this");
                    "recursive object error".to_string()
                }
            };
            format!("{}: {}", key, formatted_value)
        })
        .collect();

    // Join properties with commas and wrap in curly braces to form a Cypher-compatible object
    Ok(format!("{{{}}}", properties.join(", ")))
}

#[test]
fn test_serialize_to_cypher_object() {
    use diem_types::account_address::AccountAddress;

    // Example structs to demonstrate usage
    #[derive(Serialize)]
    struct Address {
        city: String,
        zip: String,
    }

    #[derive(Serialize)]
    struct Person {
        name: String,
        account: AccountAddress,
        age: u32,
        active: bool,
        hobbies: Vec<String>,
        address: Address, // Nested object
    }

    // Example usage with a `Person` struct that includes a nested `Address` struct and an array
    let person = Person {
        name: "Alice".to_string(),
        account: AccountAddress::ZERO,
        age: 30,
        active: true,
        hobbies: vec![
            "Reading".to_string(),
            "Hiking".to_string(),
            "Coding".to_string(),
        ],
        address: Address {
            city: "Wonderland".to_string(),
            zip: "12345".to_string(),
        },
    };

    // Serialize to a Cypher object
    let cypher_object = to_cypher_object(&person).unwrap();
    println!("{}", cypher_object);
}

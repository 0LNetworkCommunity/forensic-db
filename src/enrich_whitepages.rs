use anyhow::{Context, Result};
use diem_types::account_address::AccountAddress;
use log::{error, info};
use neo4rs::Graph;
use serde::{Deserialize, Deserializer, Serialize};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub struct Whitepages {
    #[serde(deserialize_with = "from_any_string")]
    address: Option<AccountAddress>,
    owner: Option<String>,
    address_note: Option<String>,
}

fn from_any_string<'de, D>(deserializer: D) -> Result<Option<AccountAddress>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    // do better hex decoding than this
    let mut lower = s.to_ascii_lowercase();
    if !lower.contains("0x") {
        lower = format!("0x{}", lower);
    }
    match AccountAddress::from_hex_literal(&lower) {
        Ok(addr) => Ok(Some(addr)),
        Err(_) => {
            error!("could not parse address: {}", &s);
            Ok(None)
        }
    }
}

impl Whitepages {
    pub fn parse_json_file(path: &Path) -> Result<Vec<Self>> {
        let s = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&s)?)
    }

    pub fn to_cypher_object_template(&self) -> String {
        if let Some(addr) = &self.address {
            format!(
                r#"{{owner: "{}", address: "{}"}}"#,
                self.owner.as_ref().unwrap(),
                addr.to_hex_literal(),
            )
        } else {
            error!("missing address at {:#?}", &self);
            "".to_string()
        }
    }

    /// create a cypher query string for the map object
    pub fn to_cypher_map(list: &[Self]) -> String {
        let mut list_literal = "".to_owned();
        for el in list {
            // skip empty records
            if el.owner.is_none() {
                continue;
            };

            let s = el.to_cypher_object_template();
            list_literal.push_str(&s);
            list_literal.push(',');
        }
        list_literal.pop(); // need to drop last comma ","
        format!("[{}]", list_literal)
    }

    pub fn cypher_batch_link_owner(list_str: &str) -> String {
        format!(
            r#"
  WITH {list_str} AS owner_data
  UNWIND owner_data AS each_owner

  MATCH (addr:Account {{address: each_owner.address}})

  MERGE (own:Owner {{alias: each_owner.owner}})
  MERGE (own)-[rel:Owns]->(addr)

  WITH rel
  RETURN
      COUNT(rel) AS owners_merged
"#
        )
    }
}

pub async fn impl_batch_tx_insert(pool: &Graph, batch_txs: &[Whitepages]) -> Result<u64> {
    let mut unique_owners = vec![];
    batch_txs.iter().for_each(|t| {
        if let Some(o) = &t.owner {
            if !unique_owners.contains(&o) {
                unique_owners.push(o);
            }
        }
    });

    info!("unique owner links in batch: {}", unique_owners.len());

    let list_str = Whitepages::to_cypher_map(batch_txs);

    // first insert the users
    // cypher queries makes it annoying to do a single insert of users and
    // txs
    let cypher_string = Whitepages::cypher_batch_link_owner(&list_str);

    // Execute the query
    let cypher_query = neo4rs::query(&cypher_string);
    let mut res = pool
        .execute(cypher_query)
        .await
        .context("execute query error")?;

    let row = res.next().await?.context("no row returned")?;

    let owners_merged: u64 = row.get("owners_merged").context("no owners_merged field")?;

    info!("owners linked to addresses: {}", owners_merged);

    Ok(owners_merged)
}

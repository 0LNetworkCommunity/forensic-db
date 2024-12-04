use anyhow::Result;
use chrono::{DateTime, Utc};

use serde::{Deserialize, Deserializer};

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct ExchangeOrder {
    pub user: u32,
    #[serde(rename = "orderType")]
    pub order_type: String,
    #[serde(deserialize_with = "deserialize_amount")]
    pub amount: f64,
    #[serde(deserialize_with = "deserialize_amount")]
    pub price: f64,
    pub created_at: DateTime<Utc>,
    pub filled_at: DateTime<Utc>,
    pub accepter: u32,
    #[serde(skip_deserializing)]
    pub rms_hour: f64,
    #[serde(skip_deserializing)]
    pub rms_24hour: f64,
    #[serde(skip_deserializing)]
    pub price_vs_rms_hour: f64,
    #[serde(skip_deserializing)]
    pub price_vs_rms_24hour: f64,
    #[serde(skip_deserializing)]
    pub shill_bid: Option<bool>, // New field to indicate if it took the best price
}

impl Default for ExchangeOrder {
    fn default() -> Self {
        Self {
            user: 0,
            order_type: "Sell".to_string(),
            amount: 1.0,
            price: 1.0,
            created_at: DateTime::<Utc>::from_timestamp_nanos(0),
            filled_at: DateTime::<Utc>::from_timestamp_nanos(0),
            accepter: 1,
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
            shill_bid: None,
        }
    }
}

impl ExchangeOrder {
    /// creates one transaction record in the cypher query map format
    /// Note original data was in an RFC rfc3339 with Z for UTC, Cypher seems to prefer with offsets +00000
    pub fn to_cypher_object_template(&self) -> String {
        format!(
            r#"{{user: {}, accepter: {}, order_type: "{}", amount: {}, price:{}, created_at: datetime("{}"), created_at_ts: {}, filled_at: datetime("{}"), filled_at_ts: {}, shill_bid: {}, rms_hour: {}, rms_24hour: {}, price_vs_rms_hour: {}, price_vs_rms_24hour: {} }}"#,
            self.user,
            self.accepter,
            self.order_type,
            self.amount,
            self.price,
            self.created_at.to_rfc3339(),
            self.created_at.timestamp_micros(),
            self.filled_at.to_rfc3339(),
            self.filled_at.timestamp_micros(),
            self.shill_bid.unwrap_or(false),
            self.rms_hour,
            self.rms_24hour,
            self.price_vs_rms_hour,
            self.price_vs_rms_24hour,
        )
    }

    /// create a cypher query string for the map object
    pub fn to_cypher_map(list: &[Self]) -> String {
        let mut list_literal = "".to_owned();
        for el in list {
            let s = el.to_cypher_object_template();
            list_literal.push_str(&s);
            list_literal.push(',');
        }
        list_literal.pop(); // need to drop last comma ","
        format!("[{}]", list_literal)
    }

    pub fn cypher_batch_insert_str(list_str: String) -> String {
        format!(
            r#"
  WITH {list_str} AS tx_data
  UNWIND tx_data AS tx
  MERGE (maker:SwapAccount {{swap_id: tx.user}})
  MERGE (taker:SwapAccount {{swap_id: tx.accepter}})
  MERGE (maker)-[rel:Swap {{
    order_type: tx.order_type,
    amount: tx.amount,
    price: tx.price,
    created_at: tx.created_at,
    created_at_ts: tx.created_at_ts,
    filled_at: tx.filled_at,
    filled_at_ts: tx.filled_at_ts,
    shill_bid: tx.shill_bid,
    rms_hour: tx.rms_hour,
    rms_24hour: tx.rms_24hour,
    price_vs_rms_hour: tx.price_vs_rms_hour,
    price_vs_rms_24hour: tx.price_vs_rms_24hour
  }}]->(taker)

  ON CREATE SET rel.created = true
  ON MATCH SET rel.created = false
  WITH tx, rel
  RETURN
      COUNT(CASE WHEN rel.created = true THEN 1 END) AS merged_tx_count,
      COUNT(CASE WHEN rel.created = false THEN 1 END) AS ignored_tx_count
"#
        )
    }
}

// Custom deserialization function for "amount"
fn deserialize_amount<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse::<f64>().map_err(serde::de::Error::custom)
}

pub fn deserialize_orders(json_data: &str) -> Result<Vec<ExchangeOrder>> {
    let orders: Vec<ExchangeOrder> = serde_json::from_str(json_data)?;
    Ok(orders)
}

#[test]
fn test_deserialize_orders() {
    // Raw string literal for test JSON data
    let json_data = r#"
        [
            {"user":1,"orderType":"Sell","amount":"40000.000","price":"0.00460","created_at":"2024-05-12T15:25:14.991Z","filled_at":"2024-05-14T15:04:13.000Z","accepter":3768},
            {"user":2,"orderType":"Sell","amount":"100000.000","price":"0.00994","created_at":"2024-03-11T17:23:49.860Z","filled_at":"2024-03-11T17:31:43.000Z","accepter":2440},
            {"user":3,"orderType":"Sell","amount":"50000.000","price":"0.00998","created_at":"2024-03-11T14:46:49.377Z","filled_at":"2024-03-11T14:47:12.000Z","accepter":3710},
            {"user":4,"orderType":"Buy","amount":"3027220.000","price":"0.00110","created_at":"2024-01-14T13:33:13.688Z","filled_at":"2024-01-14T18:02:44.000Z","accepter":227}
        ]
        "#;

    // Use the `deserialize_orders` function to parse the raw JSON data
    let orders = deserialize_orders(json_data).expect("Failed to deserialize orders");

    // Check that the result matches the expected values
    assert_eq!(orders.len(), 4);
    assert_eq!(orders[0].user, 1);
    assert_eq!(orders[0].order_type, "Sell");
    assert_eq!(orders[0].amount, 40000.000);
    assert_eq!(orders[0].accepter, 3768);
}

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use log::{error, trace};
use neo4rs::{Graph, Query};
// use log::trace;
// use neo4rs::{Graph, Query};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Read,
};

use crate::schema_exchange_orders::ExchangeOrder;

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct AccountDataAlt {
    pub current_balance: f64,
    pub total_funded: f64,
    pub total_outflows: f64,
    pub total_inflows: f64,
    pub daily_funding: f64,
    pub daily_inflows: f64,
    pub daily_outflows: f64,
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct UserLedger(pub HashMap<DateTime<Utc>, AccountDataAlt>);

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct BalanceTracker(pub HashMap<u32, UserLedger>); // Tracks data for each user

impl BalanceTracker {
    pub fn new() -> Self {
        BalanceTracker(HashMap::new())
    }
    /// Replay all transactions sequentially and return a balance tracker
    pub fn replay_transactions(&mut self, orders: &mut [ExchangeOrder]) -> Result<()> {
        orders.sort_by_key(|order| order.filled_at);
        for o in orders {
            self.process_transaction_alt(o);
        }
        Ok(())
    }

    pub fn process_transaction_alt(&mut self, order: &ExchangeOrder) {
        let date = order.filled_at;
        match order.order_type.as_str() {
            "Buy" => {
                // user offered to buy coins (Buyer)
                // he sends USD
                // accepter sends coins. (Seller)

                self.update_balance_and_flows_alt(order.user, date, order.amount, true);
                self.update_balance_and_flows_alt(order.accepter, date, order.amount, false);
            }
            "Sell" => {
                // user offered to sell coins (Seller)
                // he sends Coins
                // accepter sends USD. (Buyer)
                self.update_balance_and_flows_alt(order.accepter, date, order.amount, true);
                self.update_balance_and_flows_alt(order.user, date, order.amount, false);
            }
            _ => {
                println!("ERROR: not a valid Buy/Sell order, {:?}", &order);
            }
        }
    }
    fn update_balance_and_flows_alt(
        &mut self,
        user_id: u32,
        date: DateTime<Utc>,
        amount: f64,
        credit: bool,
    ) {
        let ul = self.0.entry(user_id).or_default();

        let most_recent_date = *ul.0.keys().max_by(|x, y| x.cmp(y)).unwrap_or(&date);

        // NOTE the previous record may be today's record from a previous transaction. Need to take care in the aggregation below

        // // TODO: gross, this shouldn't clone
        // let previous = if let Some(d) = most_recent_date {
        //     ul.0.entry(*).or_default().to_owned()
        // } else {
        //     AccountDataAlt::default()
        // };

        if most_recent_date > date {
            // don't know what to here
            error!("most recent ledger date is higher than current day");
            return;
        };

        let previous =
            ul.0.get(&most_recent_date)
                .unwrap_or(&AccountDataAlt::default())
                .clone();

        let today = ul.0.entry(date).or_default();

        if credit {
            today.current_balance = previous.current_balance + amount;
            today.total_inflows = previous.total_inflows + amount;
            if most_recent_date == date {
                today.daily_inflows = previous.daily_inflows + amount;
            } else {
                today.daily_inflows = amount;
            }
            // no change from on totals
            today.total_outflows = previous.total_outflows;
        } else {
            // debit
            today.current_balance = previous.current_balance - amount;
            today.total_outflows = previous.total_outflows + amount;

            if most_recent_date == date {
                today.daily_outflows = previous.daily_outflows + amount;
            } else {
                today.daily_outflows = amount;
            }

            // no change from on totals
            today.total_inflows = previous.total_inflows;
        }

        // find out if the outflows created a funding requirement on the account
        if today.current_balance < 0.0 {
            let negative_balance = today.current_balance.abs();
            // funding was needed
            today.total_funded = previous.total_funded + negative_balance;
            if most_recent_date == date {
                today.daily_funding += negative_balance;
            } else {
                today.daily_funding = negative_balance;
            }
            // reset to zero
            today.current_balance = 0.0;
        }
    }

    /// Save the balance tracker to a JSON file
    pub fn save_to_cache(&self, file_path: &str) {
        if let Ok(json) = serde_json::to_string(self) {
            let _ = fs::write(file_path, json);
        }
    }

    /// Load the balance tracker from a JSON file
    pub fn load_from_cache(file_path: &str) -> Option<Self> {
        if let Ok(mut file) = File::open(file_path) {
            let mut contents = String::new();
            if file.read_to_string(&mut contents).is_ok() {
                if let Ok(tracker) = serde_json::from_str(&contents) {
                    return Some(tracker);
                }
            }
        }
        None
    }

    pub fn to_cypher_map(&self, id: u32) -> Result<String> {
        let ul = self.0.get(&id).context("no user")?;
        let mut list_literal: String = "".to_owned();

        for date in ul.0.keys() {
            if let Some(acc) = ul.0.get(date) {
                let obj = format!(
                    r#"{{ swap_id: {}, date: "{}", current_balance: {}, total_funded: {}, total_inflows: {}, total_outflows: {}, daily_funding: {}, daily_inflows: {}, daily_outflows: {} }}"#,
                    id,
                    date.to_rfc3339(),
                    acc.current_balance,
                    acc.total_funded,
                    acc.total_inflows,
                    acc.total_outflows,
                    acc.daily_funding,
                    acc.daily_inflows,
                    acc.daily_outflows,
                );

                list_literal.push_str(&obj);
                list_literal.push(',');
            } else {
                continue;
            }
        }

        list_literal.pop(); // need to drop last comma ","
        Ok(format!("[{}]", list_literal))
    }

    /// submit to db
    pub async fn submit_ledger(&self, pool: &Graph) -> Result<u64> {
        let mut merged_relations = 0u64;
        for id in self.0.keys() {
            let data = self.to_cypher_map(*id)?;
            let query_literal = generate_cypher_query(data);
            let query = Query::new(query_literal);
            let result = pool.execute(query).await;

            match result {
                Ok(mut d) => {
                    while let r = d.next().await {
                        match r {
                            Ok(row) => {
                                if let Some(r) = row {
                                    if let Ok(i) = r.get::<u64>("merged_relations") {
                                        trace!("merged ledger in tx: {i}");
                                        merged_relations += i;
                                    };
                                }
                            }
                            Err(e) => error!("could not parse row in cypher query response: {}", e),
                        }
                    }
                }
                Err(e) => error!("could not get response in cypher query response: {}", e),
            }
        }
        Ok(merged_relations)
    }
}

/// Generate a Cypher query string to insert data into Neo4j
pub fn generate_cypher_query(map: String) -> String {
    // r#"{{ swap_id: {}, date: "{}", current_balance: {}, total_funded: {}, total_inflows: {}, total_outflows: {}, daily_funding: {}, daily_inflows: {}, daily_outflows: {} }}"#,
    format!(
        r#"
            UNWIND {map} AS account
            MERGE (sa:SwapAccount {{swap_id: account.swap_id}})
            MERGE (ul:UserLedger {{date: datetime(account.date)}})
            SET ul.current_balance = account.current_balance,
                ul.total_funded = account.total_funded,
                ul.total_inflows = account.total_inflows,
                ul.total_outflows = account.total_outflows,
                ul.daily_funding = account.daily_funding,
                ul.daily_inflows = account.daily_inflows,
                ul.daily_outflows = account.daily_outflows
            MERGE (sa)-[r:DailyLedger]->(ul)
            SET r.date = datetime(account.date)
            RETURN COUNT(r) as merged_relations
            "#,
    )
}

/// Helper function to parse "YYYY-MM-DD" into `DateTime<Utc>`
pub fn parse_date(date_str: &str) -> DateTime<Utc> {
    let datetime_str = format!("{date_str}T00:00:00Z"); // Append time and UTC offset
    DateTime::parse_from_rfc3339(&datetime_str)
        .expect("Invalid date format; expected YYYY-MM-DD")
        .with_timezone(&Utc)
}

#[test]
fn test_replay_transactions() {
    let mut orders = vec![
        // user_1 creates an offer to BUY, user_2 accepts.
        // user_1 sends USD, user_2 moves 10 coins.
        ExchangeOrder {
            user: 1,
            order_type: "Buy".to_string(),
            amount: 10.0,
            price: 2.0,
            created_at: parse_date("2024-03-01"),
            filled_at: parse_date("2024-03-02"),
            accepter: 2,
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
            shill_bid: None,
        },
        ExchangeOrder {
            // user 2 creates an offer to SELL, user 3 accepts.
            // user 3 sends USD user 2 moves amount of coins.
            user: 2,
            order_type: "Sell".to_string(),
            amount: 5.0,
            price: 3.0,
            created_at: parse_date("2024-03-05"),
            filled_at: parse_date("2024-03-06"),
            accepter: 3,
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
            shill_bid: None,
        },
        // user 3 creates an offer to BUY, user 1 accepts.
        // user 3 sends USD user 1 moves amount of coins.
        ExchangeOrder {
            user: 3,
            order_type: "Buy".to_string(),
            amount: 15.0,
            price: 1.5,
            created_at: parse_date("2024-03-10"),
            filled_at: parse_date("2024-03-11"),
            accepter: 1,
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
            shill_bid: None,
        },
    ];

    let mut tracker = BalanceTracker::new();
    tracker.replay_transactions(&mut orders).unwrap();

    let accs = tracker.0;

    let user_1 = accs.get(&1).unwrap();
    let (_, acc) = user_1.0.get_key_value(&parse_date("2024-03-02")).unwrap();

    assert!(acc.current_balance == 10.0);
    assert!(acc.total_funded == 0.0);
    assert!(acc.total_outflows == 0.0);
    assert!(acc.total_inflows == 10.0);
    assert!(acc.daily_funding == 0.0);
    assert!(acc.daily_inflows == 10.0);
    assert!(acc.daily_outflows == 0.0);

    let (_, acc) = user_1.0.get_key_value(&parse_date("2024-03-11")).unwrap();

    // balance got drawn to negative on sale of 15 coin
    assert!(acc.current_balance == 0.0);
    // implied he had to fund with at least 5 coins
    assert!(acc.total_funded == 5.0);
    assert!(acc.total_outflows == 15.0);
    // the all-time inflows should not have changed from the previous period
    assert!(acc.total_inflows == 10.0);
    assert!(acc.daily_funding == 5.0);
    assert!(acc.daily_inflows == 0.0);
    assert!(acc.daily_outflows == 15.0);

    let user_1 = accs.get(&3).unwrap();
    let (_, acc) = user_1.0.get_key_value(&parse_date("2024-03-06")).unwrap();

    assert!(acc.current_balance == 5.0);
    assert!(acc.total_funded == 0.0);
    assert!(acc.total_outflows == 0.0);
    assert!(acc.total_inflows == 5.0);
    assert!(acc.daily_funding == 0.0);
    assert!(acc.daily_inflows == 5.0);
    assert!(acc.daily_outflows == 0.0);

    let (_, acc) = user_1.0.get_key_value(&parse_date("2024-03-11")).unwrap();

    // balance should increase again
    assert!(acc.current_balance == 20.0);
    assert!(acc.total_funded == 0.0);
    assert!(acc.total_outflows == 0.0);
    assert!(acc.total_inflows == 20.0);
    assert!(acc.daily_funding == 0.0);
    assert!(acc.daily_inflows == 15.0);
    assert!(acc.daily_outflows == 0.0);
}

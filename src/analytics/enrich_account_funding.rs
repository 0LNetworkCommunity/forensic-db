use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use log::trace;
use neo4rs::{Graph, Query};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Read,
};

use crate::schema_exchange_orders::ExchangeOrder;

#[derive(Default, Debug, Deserialize, Serialize)]
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
pub struct UserLedger(HashMap<DateTime<Utc>, AccountDataAlt>);

impl UserLedger {
    pub fn process_transaction_alt(&mut self, order: &ExchangeOrder) {
        let date = order.created_at;
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
        let account = self.0.entry(&user_id).or_default();
        // let daily_balance = account.current_balance.entry(date).or_insert(0.0);
        // let day = account.day.entry(date).or_default();
        if credit {
            account.current_balance += amount;
            account.total_inflows += amount;
            account.daily_inflows += amount;
            // *daily_balance += amount
        } else {
            // debit
            account.current_balance += -amount;
            account.total_outflows += -amount;
            account.daily_outflows += -amount;
        }

        if account.current_balance < 0.0 {
            let negative_balance = account.current_balance;
            // funding was needed
            account.total_funded += negative_balance;
            account.daily_funding += negative_balance;
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
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct BalanceTracker {
    pub accounts: HashMap<u32, UserLedger>, // Tracks data for each user
}

impl BalanceTracker {
    /// Replay all transactions sequentially and return a balance tracker
    pub fn replay_transactions(orders: &mut [ExchangeOrder]) -> Self {
        let mut tracker: HashMap<u32, UserLedger> = HashMap::new();

        orders.sort_by_key(|order| order.filled_at);
        for order in sorted_orders {
            tracker.process_transaction_alt(order);
        }
        tracker
    }
    pub fn to_cypher_map(&self, id: u32) -> Result<String> {
        let ul = self.accounts.get(&id).context("no user")?;
        let mut list_literal: String = "".to_owned();

        for (date, _) in &ul.0 {
            if let Some(acc) = ul.0.get(date) {
                let obj = format!(
                  r#"{{ swap_id: {}, date: "{}", current_balance: {}, total_funded: {}, total_inflows: {}, total_outflows: {}, daily_funding: {}, daily_inflows: {}, daily_outflows: {} }}"#,
                  id,
                  date.to_rfc3339(),
                  acc.current_balance,
                  acc.total_funded
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
                ul.daily_outflows = account.daily_outflows,
            MERGE (sa)-[r:DailyLedger]->(ul)
            SET r.date = datetime(account.date)
            RETURN COUNT(r) as merged_relations
            "#,
    )
}
//     pub fn new() -> Self {
//         Self {
//             accounts: HashMap::new(),
//         }
//     }

//     pub fn process_transaction(&mut self, order: &ExchangeOrder) {
//         let date = order.created_at;
//         let (buyer_id, seller_id, amount) = match order.order_type.as_str() {
//             "Buy" => (order.user, order.accepter, order.amount * order.price),
//             "Sell" => (order.accepter, order.user, order.amount * order.price),
//             _ => {
//                 println!("ERROR: not a valid Buy/Sell order, {:?}", &order);
//                 return;
//             }
//         };

//         self.update_balance_and_flows(seller_id, date, -amount, false);
//         self.update_balance_and_flows(buyer_id, date, amount, true);
//     }

//     fn update_balance_and_flows(
//         &mut self,
//         user_id: u32,
//         date: DateTime<Utc>,
//         amount: f64,
//         credit: bool,
//     ) {
//         let account = self.accounts.entry(user_id).or_default();
//         let daily_balance = account.daily_balances.entry(date).or_insert(0.0);

//         if credit {
//             *account.daily_inflows.entry(date).or_insert(0.0) += amount;
//             *daily_balance += amount
//         } else {
//             // debit
//             *account.daily_outflows.entry(date).or_insert(0.0) += -amount;

//             *daily_balance += -amount
//         }

//         // if credit {
//         //     *account.daily_user_flows.entry(date).or_insert(0.0) += amount;
//         // } else {
//         //     *account.daily_accepter_flows.entry(date).or_insert(0.0) += amount;
//         // }

//         if *daily_balance < 0.0 {
//             // funding was needed
//             *account.daily_funding.entry(date).or_insert(0.0) += *daily_balance;
//             // reset
//             *daily_balance = 0.0;
//         }
//     }

//     /// Save the balance tracker to a JSON file
//     pub fn save_to_cache(&self, file_path: &str) {
//         if let Ok(json) = serde_json::to_string(self) {
//             let _ = fs::write(file_path, json);
//         }
//     }

//     /// Load the balance tracker from a JSON file
//     pub fn load_from_cache(file_path: &str) -> Option<Self> {
//         if let Ok(mut file) = File::open(file_path) {
//             let mut contents = String::new();
//             if file.read_to_string(&mut contents).is_ok() {
//                 if let Ok(tracker) = serde_json::from_str(&contents) {
//                     return Some(tracker);
//                 }
//             }
//         }
//         None
//     }
//     /// Generate a Cypher query string to insert data into Neo4j
//     pub fn generate_cypher_query(&self, map: String) -> String {
//         // r#"{{ swap_id: {}, date: "{}", balance: {}, funding: {}, inflows: {}, outflows: {}, user_flows: {}, accepter_flows: {} }}"#,
//         format!(
//             r#"
//             UNWIND {map} AS account
//             MERGE (sa:SwapAccount {{swap_id: account.swap_id}})
//             MERGE (ul:UserLedger {{date: datetime(account.date)}})
//             SET ul.balance = account.balance,
//                 ul.funding = account.funding,
//                 ul.inflows = account.inflows,
//                 ul.outflows = account.outflows,
//                 ul.user_flows = account.user_flows,
//                 ul.accepter_flows = account.accepter_flows
//             MERGE (sa)-[r:Daily]->(ul)
//             SET r.date = datetime(account.date)
//             RETURN COUNT(r) as merged_relations
//             "#,
//         )
//     }
// }

// /// Manages cache logic and invokes replay_transactions only if necessary
// pub fn get_or_recalculate_balances(
//     orders: &mut [ExchangeOrder],
//     cache_file: Option<String>,
//     force_recalculate: bool,
// ) -> BalanceTracker {
//     if !force_recalculate && cache_file.is_some() {
//         if let Some(cached_tracker) = BalanceTracker::load_from_cache(cache_file.as_ref().unwrap())
//         {
//             return cached_tracker;
//         }
//     }

//     let tracker = replay_transactions(orders);
//     if let Some(p) = cache_file {
//         tracker.save_to_cache(&p);
//     }
//     tracker
// }

// /// Replay all transactions sequentially and return a balance tracker
// pub fn replay_transactions(orders: &mut [ExchangeOrder]) -> BalanceTracker {
//     let mut tracker = BalanceTracker::new();
//     let sorted_orders = orders;
//     sorted_orders.sort_by_key(|order| order.filled_at);
//     for order in sorted_orders {
//         tracker.process_transaction(order);
//     }
//     tracker
// }

// /// submit to db
// pub async fn submit_ledger(balances: &BalanceTracker, pool: &Graph) -> Result<u64> {
//     let mut merged_relations = 0u64;
//     for (id, acc) in balances.accounts.iter() {
//         let data = acc.to_cypher_map(*id);
//         let query_literal = balances.generate_cypher_query(data);
//         let query = Query::new(query_literal);
//         let mut result = pool.execute(query).await?;

//         while let Some(r) = result.next().await? {
//             if let Ok(i) = r.get::<u64>("merged_relations") {
//                 trace!("merged ledger in tx: {i}");
//                 merged_relations += i;
//             };
//         }
//     }
//     Ok(merged_relations)
// }

/// Helper function to parse "YYYY-MM-DD" into `DateTime<Utc>`
fn parse_date(date_str: &str) -> DateTime<Utc> {
    let datetime_str = format!("{date_str}T00:00:00Z"); // Append time and UTC offset
    DateTime::parse_from_rfc3339(&datetime_str)
        .expect("Invalid date format; expected YYYY-MM-DD")
        .with_timezone(&Utc)
}
// /// Reusable function to print account data
// pub fn print_account_data(user_id: u32, data: &AccountData) {
//     println!("User: {}", user_id);
//     for (date, balance) in &data.daily_balances {
//         println!("  Date: {}, Balance: {}", date, balance);
//     }
//     for (date, funding) in &data.daily_funding {
//         println!("  Date: {}, Funding: {}", date, funding);
//     }
//     for (date, inflow) in &data.daily_inflows {
//         println!("  Date: {}, Inflows: {}", date, inflow);
//     }
//     for (date, outflow) in &data.daily_outflows {
//         println!("  Date: {}, Outflows: {}", date, outflow);
//     }
//     for (date, user_flow) in &data.daily_user_flows {
//         println!("  Date: {}, User Flow: {}", date, user_flow);
//     }
//     for (date, accepter_flow) in &data.daily_accepter_flows {
//         println!("  Date: {}, Accepter Flow: {}", date, accepter_flow);
//     }
// }

// /// Display statistics for a specific account within a date range
// pub fn display_account_statistics(
//     user_id: u32,
//     data: &AccountData,
//     start_date: &str,
//     end_date: &str,
// ) {
//     let start = parse_date(start_date);
//     let end = parse_date(end_date);

//     println!(
//         "Statistics for User {} from {} to {}",
//         user_id, start_date, end_date
//     );

//     let mut total_balance = 0.0;
//     let mut total_funding = 0.0;
//     let mut total_inflows = 0.0;
//     let mut total_outflows = 0.0;

//     for (date, balance) in &data.daily_balances {
//         if *date >= start && *date <= end {
//             total_balance += balance;
//         }
//     }

//     for (date, funding) in &data.daily_funding {
//         if *date >= start && *date <= end {
//             total_funding += funding;
//         }
//     }

//     for (date, inflow) in &data.daily_inflows {
//         if *date >= start && *date <= end {
//             total_inflows += inflow;
//         }
//     }

//     for (date, outflow) in &data.daily_outflows {
//         if *date >= start && *date <= end {
//             total_outflows += outflow;
//         }
//     }

//     println!("  Total Balance: {:.2}", total_balance);
//     println!("  Total Funding: {:.2}", total_funding);
//     println!("  Total Inflows: {:.2}", total_inflows);
//     println!("  Total Outflows: {:.2}", total_outflows);
// }

#[test]
fn test_replay_transactions() {
    let mut orders = vec![
        // user 1 creates an offer to BUY, user 2 accepts.
        // user 1 sends USD user 2 move amount of coins.
        ExchangeOrder {
            user: 1,
            order_type: "BUY".to_string(),
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
            order_type: "SELL".to_string(),
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
            order_type: "BUY".to_string(),
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

    let tracker = BalanceTracker::replay_transactions(&mut orders);

    // // Analyze results for March 2024
    // for (user_id, data) in &tracker.accounts {
    //     print_account_data(*user_id, data);
    //     display_account_statistics(*user_id, data, "2024-03-01", "2024-03-31");
    // }
}

// #[ignore]
// // TODO: check paths
// #[test]
// fn test_cache_mechanism() {
//     let cache_file = "balance_tracker_cache.json".to_string();
//     let mut orders = vec![
//         ExchangeOrder {
//             user: 1,
//             order_type: "BUY".to_string(),
//             amount: 10.0,
//             price: 2.0,
//             created_at: parse_date("2024-03-01"),
//             filled_at: parse_date("2024-03-02"),
//             accepter: 2,
//             rms_hour: 0.0,
//             rms_24hour: 0.0,
//             price_vs_rms_hour: 0.0,
//             price_vs_rms_24hour: 0.0,
//             shill_bid: None,
//         },
//         ExchangeOrder {
//             user: 2,
//             order_type: "SELL".to_string(),
//             amount: 5.0,
//             price: 3.0,
//             created_at: parse_date("2024-03-05"),
//             filled_at: parse_date("2024-03-06"),
//             accepter: 3,
//             rms_hour: 0.0,
//             rms_24hour: 0.0,
//             price_vs_rms_hour: 0.0,
//             price_vs_rms_24hour: 0.0,
//             shill_bid: None,
//         },
//     ];

//     let tracker = get_or_recalculate_balances(&mut orders, Some(cache_file.clone()), true);
//     assert!(tracker.accounts.contains_key(&1));
//     assert!(tracker.accounts.contains_key(&2));

//     // Test loading from cache
//     let cached_tracker = get_or_recalculate_balances(&mut orders, Some(cache_file.clone()), false);
//     assert!(cached_tracker.accounts.contains_key(&1));
//     assert!(cached_tracker.accounts.contains_key(&2));

//     // Cleanup
//     let _ = fs::remove_file(cache_file);
// }

// // #[test]

// // fn test_cypher_query() {
// //     let tracker = BalanceTracker::new(); // Assume tracker is populated
// //                                          // let params = tracker.generate_cypher_params();
// //     let query = tracker.generate_cypher_query();
// //     // dbg!(&params);
// //     dbg!(&query);
// // }

// // I'm coding some data analysis in rust.

// // I have a vector structs that looks like this:

// // pub struct ExchangeOrder {
// //     pub user: u32,
// //     #[serde(rename = "orderType")]
// //     pub order_type: String,
// //     #[serde(deserialize_with = "deserialize_amount")]
// //     pub amount: f64,
// //     #[serde(deserialize_with = "deserialize_amount")]
// //     pub price: f64,
// //     pub created_at: DateTime<Utc>,
// //     pub filled_at: DateTime<Utc>,
// //     pub accepter: u32,
// //     #[serde(skip_deserializing)]
// //     pub rms_hour: f64,
// //     #[serde(skip_deserializing)]
// //     pub rms_24hour: f64,
// //     #[serde(skip_deserializing)]
// //     pub price_vs_rms_hour: f64,
// //     #[serde(skip_deserializing)]
// //     pub price_vs_rms_24hour: f64,
// //     #[serde(skip_deserializing)]
// //     pub shill_bid: Option<bool>, // New field to indicate if it took the best price
// // }

// // My goal is to determine the amount of funding ('amount') that each account required at a given time. We will need to replay all the transaction history sequentially.

// // We need a new data structure to track account balances. Accepting a BUY transaction by another User, would decrease the total balance of the accepter, and increase of the User. Accepting a SELL transaction, would increase the balance of the accepter, and decrease that of the User.

// // We also need a data structure to save when there were funding events to the account. We can assume all accounts start at 0 total_balance. This means that we need to also track a funded_event_amount, for whenever the account would have a negative balance.

// // As for granularity of time we should just track daily balances, and daily funding.

// // How would I do this in Rust?

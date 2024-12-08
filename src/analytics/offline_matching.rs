use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use diem_types::account_address::AccountAddress;
use neo4rs::Graph;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Deposit {
    account: AccountAddress,
    deposited: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MinFunding {
    user_id: u32,
    funded: f64,
}

pub async fn get_date_range_deposits(
    pool: &Graph,
    top_n: u64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Deposit>> {
    let mut top_deposits = vec![];

    let q = format!(
        r#"
        WITH "0xf57d3968d0bfd5b3120fda88f34310c70bd72033f77422f4407fbbef7c24557a" as exchange_deposit
        MATCH
          (u:Account)-[tx:Tx]->(onboard:Account {{address: exchange_deposit}})
        WHERE
          tx.`block_datetime` > datetime("{}")
          AND tx.`block_datetime` < datetime("{}")
        WITH
          u,
          SUM(tx.V7_OlAccountTransfer_amount) AS totalTxAmount
        ORDER BY totalTxAmount DESCENDING
        RETURN u.address AS account, toFloat(totalTxAmount) / 1000000 AS deposited
        LIMIT {}
        "#,
        start.to_rfc3339(),
        end.to_rfc3339(),
        top_n,
    );
    let cypher_query = neo4rs::query(&q);

    // Execute the query
    let mut result = pool.execute(cypher_query).await?;

    // Fetch the first row only
    while let Some(r) = result.next().await? {
        let account_str = r.get::<String>("account").unwrap_or("unknown".to_string());
        let deposited = r.get::<f64>("deposited").unwrap_or(0.0);
        let d = Deposit {
            account: account_str.parse().unwrap_or(AccountAddress::ZERO),
            deposited,
        };
        top_deposits.push(d);
        // dbg!(&d);
    }
    Ok(top_deposits)
}

pub async fn get_min_funding(
    pool: &Graph,
    top_n: u64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<MinFunding>> {
    let mut min_funding = vec![];

    let q = format!(
        r#"
        MATCH p=(e:SwapAccount)-[d:DailyLedger]-(ul:UserLedger)
        WHERE d.date > datetime("{}")
              AND d.date < datetime("{}")
        WITH e.swap_id AS user_id, toFloat(max(ul.`total_funded`)) as funded
        RETURN user_id, funded
        ORDER BY funded DESC
        LIMIT {}
        "#,
        start.to_rfc3339(),
        end.to_rfc3339(),
        top_n,
    );
    let cypher_query = neo4rs::query(&q);

    // Execute the query
    let mut result = pool.execute(cypher_query).await?;

    // Fetch the first row only
    while let Some(r) = result.next().await? {
        let user_id = r.get::<u32>("user_id").unwrap_or(0);
        let funded = r.get::<f64>("funded").unwrap_or(0.0);
        let d = MinFunding { user_id, funded };
        min_funding.push(d);
        // dbg!(&d);
    }
    Ok(min_funding)
}

#[derive(Clone, Default, Debug)]
pub struct Candidates {
    maybe: Vec<AccountAddress>,
    impossible: Vec<AccountAddress>,
}

#[derive(Clone, Default, Debug)]
pub struct Matching(pub BTreeMap<u32, Candidates>);

impl Matching {
    pub fn new() -> Self {
        Matching(BTreeMap::new())
    }

    pub fn match_deposit_to_funded(&mut self, deposits: Vec<Deposit>, funded: Vec<MinFunding>) {
        for f in funded.iter() {
            deposits.iter().for_each(|d| {
                let candidates = self.0.entry(f.user_id).or_default();
                // only addresses with minimum funded could be a Maybe
                if d.deposited >= f.funded {
                    candidates.maybe.push(d.account);
                } else {
                    candidates.impossible.push(d.account);
                }
            });
        }
    }
}

pub async fn rip_range(pool: &Graph, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Matching> {
    let mut matches = Matching::new();

    // loop each day.
    for d in days_in_range(start, end) {
        let deposits = get_date_range_deposits(pool, 100, start, d).await?;
        let funded = get_min_funding(pool, 20, start, d).await?;

        matches.match_deposit_to_funded(deposits, funded);
    }

    Ok(matches)
}

fn days_in_range(start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<DateTime<Utc>> {
    let mut days = Vec::new();
    let mut current = start;

    while current <= end {
        days.push(current);
        current += Duration::days(1); // Increment by one day
    }

    days
}

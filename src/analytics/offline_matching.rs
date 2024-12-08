use anyhow::Result;
use chrono::{DateTime, Utc};
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

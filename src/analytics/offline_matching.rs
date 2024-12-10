use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};
use diem_types::account_address::AccountAddress;
use log::{info, trace};
use neo4rs::Graph;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Deposit {
    account: AccountAddress,
    deposited: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MinFunding {
    pub user_id: u32,
    pub funded: f64,
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

pub async fn get_exchange_users(
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

pub async fn get_one_exchange_user(
    pool: &Graph,
    id: u32,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<MinFunding>> {
    let mut min_funding = vec![];

    let q = format!(
        r#"
        MATCH p=(e:SwapAccount)-[d:DailyLedger]-(ul:UserLedger)
        WHERE d.date > datetime("{}")
              AND d.date < datetime("{}")
              AND e.swap_id = {}
        WITH DISTINCT(e.swap_id) AS user_id, toFloat(max(ul.`total_funded`)) as funded
        RETURN user_id, funded
        "#,
        start.to_rfc3339(),
        end.to_rfc3339(),
        id,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Matching {
    pub definite: BTreeMap<u32, AccountAddress>,
    pub pending: BTreeMap<u32, Candidates>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Candidates {
    pub maybe: Vec<AccountAddress>,
    pub impossible: Vec<AccountAddress>,
}

#[derive(Clone, Default, Debug)]
pub struct Possible {
    pub user: Vec<u32>,
    pub address: Vec<AccountAddress>,
}

impl Default for Matching {
    fn default() -> Self {
        Self::new()
    }
}

impl Matching {
    pub fn new() -> Self {
        Self {
            definite: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    pub fn get_next_search_ids(&self, funded: &[MinFunding]) -> Result<(u32, u32)> {
        // assumes this is sorted by date

        // find the next two which are not identified, to disambiguate.
        let ids: Vec<u32> = funded
            .iter()
            .filter(|el| !self.definite.contains_key(&el.user_id))
            .take(2)
            .map(|el| el.user_id)
            .collect();

        dbg!(&ids);
        // let user_ledger = funded.iter().find(|el| {
        //   // check if we have already identified it
        //   self.definite.0.get(el.user_id).none()
        // });
        Ok((*ids.first().unwrap(), *ids.get(1).unwrap()))
    }

    /// progressively scan for top_n funded exchange accounts
    /// e.g. start with 5, and each time increase by 1, until reaching 50 for.
    /// at each level deep, a breadth search is started
    /// Thus every day in timeseries will do a shallow match of the top 5 accounts, and  eliminate candidates. Deeper searches benefit from the information from the previous depth searches (exclude impossible matches)
    pub async fn depth_search_by_top_n_accounts(
        &mut self,
        pool: &Graph,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        save_dir: Option<PathBuf>,
    ) -> Result<()> {
        let mut top_n = 10;
        while top_n < 50 {
            let _ = self
                .breadth_search_by_dates(pool, top_n, start, end, &save_dir)
                .await; // don't error
            top_n += 5;
        }
        Ok(())
    }

    /// breadth search, for every day in timeseries, check all the top funded
    /// accounts against the actual deposited on-chain
    /// don't peek into the future, only replay information at each day
    pub async fn breadth_search_by_dates(
        &mut self,
        pool: &Graph,
        top_n: u64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        save_dir: &Option<PathBuf>,
    ) -> Result<()> {
        // expand the search
        // increase the search of top users by funding by expanding the window
        // this may retry a number of users, but with more users discovered
        // the search space gets smaller
        for d in days_in_range(start, end) {
            info!("day: {}", d);
            let next_list = get_exchange_users(pool, top_n, start, d).await?;

            // TODO: pick top of deposits
            let deposits = get_date_range_deposits(pool, 100, start, d).await?;

            for u in next_list {
                let _r = self.search(&u, &deposits).await;

                // after each loop update the file
                if let Some(p) = &save_dir {
                    let _ = self.write_definite_to_file(&p.join("definite.json"));
                    let _ = self.write_cache_to_file(p);
                }
            }
        }

        Ok(())
    }
    pub async fn search(
        &mut self,
        user: &MinFunding,
        deposits: &[Deposit],
    ) -> Result<AccountAddress> {
        // exit early
        if let Some(a) = self.definite.get(&user.user_id) {
            return Ok(*a);
        }

        self.eliminate_candidates(user, deposits);

        if let Some(a) = self.definite.get(&user.user_id) {
            return Ok(*a);
        }

        bail!("could not find a candidate")
    }

    pub fn eliminate_candidates(&mut self, user: &MinFunding, deposits: &[Deposit]) {
        // let mut filtered_depositors = deposits.clone();
        let pending = self.pending.entry(user.user_id).or_default();

        let mut eval: Vec<AccountAddress> = vec![];
        deposits.iter().for_each(|el| {
            if el.deposited >= user.funded &&
            // must not already have been tagged impossible
            !pending.impossible.contains(&el.account) &&
            // is also not already discovered
            !self.definite.values().any(|found| found == &el.account)
            {
                if !eval.contains(&el.account) {
                    eval.push(el.account)
                }
            } else if !pending.impossible.contains(&el.account) {
                pending.impossible.push(el.account)
            }
        });

        // only increment the first time.
        if pending.maybe.is_empty() {
            pending.maybe.append(&mut eval);
        } else {
            // we only keep addresses we see repeatedly (inner join)
            eval.retain(|x| pending.maybe.contains(x));
            if !eval.is_empty() {
                pending.maybe = eval;
            }
        }

        info!("user: {}, maybe: {}", &user.user_id, &pending.maybe.len());

        if pending.maybe.len() == 1 {
            // we found a definite match, update it so the next loop doesn't include it
            self.definite
                .insert(user.user_id, *pending.maybe.first().unwrap());
        }

        // candidates
    }

    pub fn write_cache_to_file(&self, dir: &Path) -> Result<()> {
        let json_string = serde_json::to_string(&self).expect("Failed to serialize");

        // Save the JSON string to a file
        let path = dir.join("cache.json");
        let mut file = File::create(&path)?;
        file.write_all(json_string.as_bytes())?;

        trace!("Cache saved: {}", path.display());
        Ok(())
    }
    pub fn read_cache_from_file(dir: &Path) -> Result<Self> {
        // Read the file content into a string
        let file_path = dir.join("cache.json");
        let json_string = fs::read_to_string(file_path)?;

        // Deserialize the JSON string into a BTreeMap
        Ok(serde_json::from_str(&json_string)?)
    }

    pub fn write_definite_to_file(&self, path: &Path) -> Result<()> {
        // Serialize the BTreeMap to a JSON string
        let json_string =
            serde_json::to_string_pretty(&self.definite).expect("Failed to serialize");

        // Save the JSON string to a file
        let mut file = File::create(path)?;
        file.write_all(json_string.as_bytes())?;

        trace!("Data saved to path: {}", path.display());
        Ok(())
    }
}

pub fn sort_funded(funded: &mut [MinFunding]) {
    // sort descending
    funded.sort_by(|a, b| b.funded.partial_cmp(&a.funded).unwrap());
}

pub fn days_in_range(start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<DateTime<Utc>> {
    let mut days = Vec::new();
    let mut current = start;

    while current <= end {
        days.push(current);
        current += Duration::days(1); // Increment by one day
    }

    days
}

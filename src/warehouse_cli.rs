use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use log::info;
use neo4rs::Graph;
use serde_json::json;
use std::path::PathBuf;

use crate::{
    analytics,
    enrich_exchange_onboarding::{self, ExchangeOnRamp},
    enrich_whitepages::{self, Whitepages},
    json_rescue_v5_load,
    load::{ingest_all, try_load_one_archive},
    load_exchange_orders,
    neo4j_init::{self, get_credentials_from_env, PASS_ENV, URI_ENV, USER_ENV},
    scan::{scan_dir_archive, BundleContent},
};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(arg_required_else_help(true))]
/// Extract, transform, and load data into a graph datawarehouse
pub struct WarehouseCli {
    #[clap(long, short('r'))]
    /// URI of graphDB e.g. neo4j+s://localhost:port
    db_uri: Option<String>,
    #[clap(long, short('u'))]

    /// username of db
    db_username: Option<String>,
    #[clap(long, short('p'))]
    /// db password
    db_password: Option<String>,
    #[clap(long, short('q'))]
    /// force clear queue
    clear_queue: bool,

    #[clap(long, short('t'))]
    /// max tasks to run in parallel
    threads: Option<usize>,

    #[clap(subcommand)]
    command: Sub,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Sub {
    /// scans sub directories for archive bundles
    IngestAll {
        #[clap(long, short('d'))]
        /// path to start crawling from
        start_path: PathBuf,
        #[clap(long, short('c'))]
        /// type of content to load
        archive_content: Option<BundleContent>,
        #[clap(long, short('b'))]
        /// size of each batch to load
        batch_size: Option<usize>,
    },
    /// process and load a single archive
    LoadOne {
        #[clap(long, short('d'))]
        /// location of archive
        archive_dir: PathBuf,

        #[clap(long, short('b'))]
        /// size of each batch to load
        batch_size: Option<usize>,
    },
    /// check archive is valid and can be decoded
    Check {
        #[clap(long, short('d'))]
        archive_dir: PathBuf,
    },
    /// add supporting data in addition to chain records
    EnrichExchange {
        #[clap(long)]
        /// file with swap records
        exchange_json: PathBuf,
        #[clap(long)]
        /// size of each batch to load
        batch_size: Option<usize>,
    },
    /// link an onboarding address to an exchange ID
    EnrichExchangeOnramp {
        #[clap(long)]
        /// file with onboarding accounts
        onboarding_json: PathBuf,
    },
    /// map owners of accounts from json file
    EnrichWhitepages {
        #[clap(long)]
        /// file with owner map
        owner_json: PathBuf,
    },
    VersionFiveTx {
        #[clap(long)]
        /// starting path for v5 .tgz files
        archive_dir: PathBuf,
    },
    #[clap(subcommand)]
    Analytics(AnalyticsSub),
}

#[derive(Subcommand)]

pub enum AnalyticsSub {
    ExchangeRMS {
        #[clap(long)]
        /// commits the analytics to the db
        commit: bool,
    },
}

impl WarehouseCli {
    pub async fn run(&self) -> anyhow::Result<()> {
        match &self.command {
            Sub::IngestAll {
                start_path,
                archive_content,
                batch_size,
            } => {
                let map = scan_dir_archive(start_path, archive_content.to_owned())?;
                let pool = try_db_connection_pool(self).await?;
                neo4j_init::maybe_create_indexes(&pool).await?;
                ingest_all(&map, &pool, self.clear_queue, batch_size.unwrap_or(250)).await?;
            }
            Sub::LoadOne {
                archive_dir,
                batch_size,
            } => match scan_dir_archive(archive_dir, None)?.0.get(archive_dir) {
                Some(man) => {
                    let pool = try_db_connection_pool(self).await?;
                    neo4j_init::maybe_create_indexes(&pool).await?;

                    try_load_one_archive(man, &pool, batch_size.unwrap_or(250)).await?;
                }
                None => {
                    bail!(format!(
                        "ERROR: cannot find .manifest file under {}",
                        archive_dir.display()
                    ));
                }
            },
            Sub::Check { archive_dir } => {
                match scan_dir_archive(archive_dir, None)?.0.get(archive_dir) {
                    Some(_) => todo!(),
                    None => {
                        bail!(format!(
                            "ERROR: cannot find .manifest file under {}",
                            archive_dir.display()
                        ));
                    }
                }
            }
            Sub::EnrichExchange {
                exchange_json: swap_record_json,
                batch_size,
            } => {
                let pool = try_db_connection_pool(self).await?;
                neo4j_init::maybe_create_indexes(&pool).await?;

                load_exchange_orders::load_from_json(
                    swap_record_json,
                    &pool,
                    batch_size.unwrap_or(250),
                )
                .await?;
            }
            Sub::EnrichExchangeOnramp { onboarding_json } => {
                info!("exchange onramp");
                let pool = try_db_connection_pool(self).await?;

                let wp = ExchangeOnRamp::parse_json_file(onboarding_json)?;
                let owners_merged =
                    enrich_exchange_onboarding::impl_batch_tx_insert(&pool, &wp).await?;

                println!("SUCCESS: {} exchange onramp accounts linked", owners_merged);
            }
            Sub::EnrichWhitepages {
                owner_json: json_file,
            } => {
                info!("whitepages");
                let pool = try_db_connection_pool(self).await?;

                let wp = Whitepages::parse_json_file(json_file)?;
                let owners_merged = enrich_whitepages::impl_batch_tx_insert(&pool, &wp).await?;

                println!("SUCCESS: {} owner accounts linked", owners_merged);
            }
            Sub::VersionFiveTx { archive_dir } => {
                let pool = try_db_connection_pool(self).await?;

                json_rescue_v5_load::rip_concurrent_limited(
                    archive_dir,
                    &pool,
                    self.threads.to_owned(),
                )
                .await?;
            }
            Sub::Analytics(analytics_sub) => match analytics_sub {
                AnalyticsSub::ExchangeRMS { commit } => {
                    info!("ExchangeRMS: {}", commit);
                    let pool = try_db_connection_pool(self).await?;
                    let results = analytics::exchange_stats::query_rms_analytics_concurrent(
                        &pool, None, None,
                    )
                    .await?;
                    println!("{:#}", json!(&results).to_string());
                }
            },
        };
        Ok(())
    }
}

pub async fn try_db_connection_pool(cli: &WarehouseCli) -> Result<Graph> {
    let db = match get_credentials_from_env() {
        Ok((uri, user, password)) => Graph::new(uri, user, password).await?,
        Err(_) => {
            if cli.db_uri.is_some() && cli.db_username.is_some() && cli.db_password.is_some() {
                Graph::new(
                    cli.db_uri.as_ref().unwrap(),
                    cli.db_username.as_ref().unwrap(),
                    cli.db_password.as_ref().unwrap(),
                )
                .await?
            } else {
                println!("Must pass DB credentials, either with CLI args or environment variable");
                println!("call with --db-uri, --db-user, and --db-password");
                println!(
                    "Alternatively export credentials to env variables: {}, {}, {}",
                    URI_ENV, USER_ENV, PASS_ENV
                );
                bail!("could not get a db instance with credentials");
            }
        }
    };
    Ok(db)
}

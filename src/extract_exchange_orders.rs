use anyhow::Result;
use std::{fs::File, io::Read, path::Path};

use log::info;

use crate::schema_exchange_orders::{self, ExchangeOrder};

pub fn read_orders_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<ExchangeOrder>> {
    let mut file = File::open(path)?;
    let mut json_data = String::new();
    file.read_to_string(&mut json_data)?;
    let des = schema_exchange_orders::deserialize_orders(&json_data)?;

    info!("swap orders extracted from file: {}", des.len());

    Ok(des)
}

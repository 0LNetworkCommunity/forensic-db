use chrono::{DateTime, Utc};
use diem_types::account_address::AccountAddress;
use log::error;
use serde::{Deserialize, Deserializer};

// TODO check decimal precision
/// Conversion of coins from V5 to V6
pub const LEGACY_REBASE_MULTIPLIER: u64 = 35;
/// Decimal precision
// TODO: duplication, this is probably defined in libra-framework somewhere
pub const COIN_DECIMAL_PRECISION: u64 = 1000000;

/// Helper function to parse "YYYY-MM-DD" into `DateTime<Utc>`
pub fn parse_date(date_str: &str) -> DateTime<Utc> {
    let datetime_str = format!("{date_str}T00:00:00Z"); // Append time and UTC offset
    DateTime::parse_from_rfc3339(&datetime_str)
        .expect("Invalid date format; expected YYYY-MM-DD")
        .with_timezone(&Utc)
}

pub fn de_address_from_any_string<'de, D>(
    deserializer: D,
) -> Result<Option<AccountAddress>, D::Error>
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

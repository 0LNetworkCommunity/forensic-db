use chrono::{DateTime, Utc};
/// Helper function to parse "YYYY-MM-DD" into `DateTime<Utc>`
pub fn parse_date(date_str: &str) -> DateTime<Utc> {
    let datetime_str = format!("{date_str}T00:00:00Z"); // Append time and UTC offset
    DateTime::parse_from_rfc3339(&datetime_str)
        .expect("Invalid date format; expected YYYY-MM-DD")
        .with_timezone(&Utc)
}

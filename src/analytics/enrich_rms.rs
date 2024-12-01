use chrono::{DateTime, Duration, Utc};
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct Swap {
    pub from_user: String,
    pub to_accepter: String,
    pub filled_at: DateTime<Utc>,
    pub amount: f64,
    pub created_at: DateTime<Utc>,
    pub price: f64,
    pub created: bool,
    pub order_type: String,
    pub rms_hour: f64,
    pub rms_24hour: f64,
    pub price_vs_rms_hour: f64,
    pub price_vs_rms_24hour: f64,
}

fn calculate_rms(data: &[f64]) -> f64 {
    let (sum, count) = data
        .iter()
        .fold((0.0, 0), |(sum, count), x| (sum + (x * x), count + 1));
    if count > 0 {
        (sum / count as f64).sqrt()
    } else {
        0.0
    }
}

/// enrich swap struct with RMS data
pub fn process_swaps(swaps: &mut [Swap]) {
    swaps.sort_by_key(|swap| swap.filled_at);

    let mut window_1hour: VecDeque<Swap> = VecDeque::new();
    let mut window_24hour: VecDeque<Swap> = VecDeque::new();

    let one_hour = Duration::hours(1);
    let twenty_four_hours = Duration::hours(24);

    for swap in swaps.iter_mut() {
        let current_time = swap.filled_at;

        // Remove outdated transactions
        while let Some(front) = window_1hour.front() {
            if (current_time - front.filled_at) > one_hour {
                window_1hour.pop_front();
            } else {
                break;
            }
        }

        while let Some(front) = window_24hour.front() {
            if current_time - front.filled_at > twenty_four_hours {
                window_24hour.pop_front();
            } else {
                break;
            }
        }

        // Add current swap to windows
        window_1hour.push_back(swap.clone());
        window_24hour.push_back(swap.clone());

        // Collect filtered amounts before borrowing swap mutably
        let filtered_1hour: Vec<f64> = window_1hour
            .iter()
            .filter(|s| s.from_user != swap.from_user && s.to_accepter != swap.to_accepter)
            .map(|s| s.price)
            .collect();

        let filtered_24hour: Vec<f64> = window_24hour
            .iter()
            .filter(|s| s.from_user != swap.from_user && s.to_accepter != swap.to_accepter)
            .map(|s| s.price)
            .collect();

        // Now we can safely borrow swap mutably
        swap.rms_hour = calculate_rms(&filtered_1hour);
        swap.rms_24hour = calculate_rms(&filtered_24hour);

        // Calculate percentages
        swap.price_vs_rms_hour = if swap.rms_hour > 0.0 {
            swap.price / swap.rms_hour
        } else {
            0.0
        };

        swap.price_vs_rms_24hour = if swap.rms_24hour > 0.0 {
            swap.price / swap.rms_24hour
        } else {
            0.0
        };
    }
}

#[test]
fn test_rms_pipeline() {
    let mut swaps = vec![
        // first trade 5/5/2024 8pm
        Swap {
            from_user: "Alice".into(),
            to_accepter: "Bob".into(),
            filled_at: DateTime::parse_from_rfc3339("2024-05-05T20:02:00Z")
                .unwrap()
                .with_timezone(&Utc),
            amount: 40000.0,
            created_at: DateTime::parse_from_rfc3339("2024-05-01T05:46:13.508Z")
                .unwrap()
                .with_timezone(&Utc),
            price: 100.0,
            created: true,
            order_type: "Buy".into(),
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
        },
        // less than 12 hours later next trade 5/6/2024 8AM
        Swap {
            from_user: "Alice".into(),
            to_accepter: "Bob".into(),
            filled_at: DateTime::parse_from_rfc3339("2024-05-06T08:01:00Z")
                .unwrap()
                .with_timezone(&Utc),
            amount: 40000.0,
            created_at: DateTime::parse_from_rfc3339("2024-05-01T05:46:13.508Z")
                .unwrap()
                .with_timezone(&Utc),
            price: 4.0,
            created: true,
            order_type: "Buy".into(),
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
        },
        // less than one hour later
        Swap {
            from_user: "Alice".into(),
            to_accepter: "Bob".into(),
            filled_at: DateTime::parse_from_rfc3339("2024-05-06T09:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            amount: 40000.0,
            created_at: DateTime::parse_from_rfc3339("2024-05-01T05:46:13.508Z")
                .unwrap()
                .with_timezone(&Utc),
            price: 4.0,
            created: true,
            order_type: "Buy".into(),
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
        },
        // same time as previous but different traders
        Swap {
            from_user: "Carol".into(),
            to_accepter: "Dave".into(),
            filled_at: DateTime::parse_from_rfc3339("2024-05-06T09:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            amount: 25000.0,
            created_at: DateTime::parse_from_rfc3339("2024-05-01T03:46:13.508Z")
                .unwrap()
                .with_timezone(&Utc),
            price: 32.0,
            created: true,
            order_type: "Sell".into(),
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
        },
    ];

    process_swaps(&mut swaps);

    // for swap in swaps.iter() {
    //     println!("{:?}", swap);
    // }

    let s0 = swaps.get(0).unwrap();
    assert!(s0.rms_hour == 0.0);
    assert!(s0.rms_24hour == 0.0);
    let s1 = swaps.get(1).unwrap();
    assert!(s1.rms_hour == 0.0);
    assert!(s1.rms_24hour == 0.0);
    let s2 = swaps.get(2).unwrap();
    assert!(s2.rms_hour == 0.0);
    assert!(s2.rms_24hour == 0.0);
    let s3 = swaps.get(3).unwrap();
    assert!(s3.rms_hour == 4.0);
    assert!((s3.rms_24hour > 57.0) && (s3.rms_24hour < 58.0));
}

use chrono::Duration;
use std::collections::VecDeque;

use crate::schema_exchange_orders::{CompetingOffers, ExchangeOrder, OrderType};

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
pub fn include_rms_stats(swaps: &mut [ExchangeOrder]) {
    swaps.sort_by_key(|swap| swap.filled_at);

    let mut window_1hour: VecDeque<ExchangeOrder> = VecDeque::new();
    let mut window_24hour: VecDeque<ExchangeOrder> = VecDeque::new();

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
            .filter(|s| s.user != swap.user && s.accepter != swap.accepter)
            .map(|s| s.price)
            .collect();

        let filtered_24hour: Vec<f64> = window_24hour
            .iter()
            .filter(|s| s.user != swap.user && s.accepter != swap.accepter)
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

fn get_competing_offers(
    current_order: &ExchangeOrder,
    all_offers: &[ExchangeOrder],
) -> CompetingOffers {
    let mut competition = CompetingOffers {
        offer_type: current_order.order_type.clone(),
        ..Default::default()
    };

    for other in all_offers {
        if competition.offer_type != other.order_type {
            continue;
        }

        // is the other offer created in the past, and still not filled
        if other.created_at < current_order.filled_at && other.filled_at > current_order.filled_at {
            competition.open_same_type += 1;
            if other.amount <= current_order.amount {
                competition.within_amount += 1;

                if other.price <= current_order.price {
                    competition.within_amount_lower_price += 1;
                }
            }
        }
    }

    competition
}
pub fn process_shill(all_transactions: &mut [ExchangeOrder]) {
    all_transactions.sort_by_key(|el| el.filled_at); // Sort by filled_at

    // TODO: gross, see what you make me do, borrow checker.
    let temp_tx = all_transactions.to_vec();

    for current_order in all_transactions.iter_mut() {
        let comp = get_competing_offers(current_order, &temp_tx);

        // We can only evaluate if an "accepter" is engaged in shill behavior.
        // the "offerer" may create unreasonable offers, but the shill trade requires someone accepting.

        match comp.offer_type {
            // An accepter may be looking to dispose of coins.
            // They must fill someone else's "BUY" offer.

            // Rationally would want to dispose at the highest price possible.
            // so if we find that there were more HIGHER offers to buy which this accepter did not take, we must wonder why they are taking a lower price voluntarily.
            // it would indicate they are shilling_down
            OrderType::Buy => {
                if let Some(higher_priced_orders) = comp
                    .within_amount
                    .checked_sub(comp.within_amount_lower_price)
                {
                    if higher_priced_orders > 0 {
                        current_order.accepter_shill_down = true
                    }
                }
                // Similarly an accepter may be looking to accumulate coins.
                // They rationally will do so at the lowest price available
                // We want to check if they are ignoring lower priced offers
                // of the same or lower amount.
                // If so it means they are pushing the price up.
            }
            OrderType::Sell => {
                if comp.within_amount_lower_price > 0 {
                    current_order.accepter_shill_up = true
                }
            }
        }
    }
}

#[test]
fn test_rms_pipeline() {
    use chrono::{DateTime, Utc};
    let mut swaps = vec![
        // first trade 5/5/2024 8pm
        ExchangeOrder {
            user: 1,     // alice
            accepter: 2, // bob
            filled_at: DateTime::parse_from_rfc3339("2024-05-05T20:02:00Z")
                .unwrap()
                .with_timezone(&Utc),
            amount: 40000.0,
            created_at: DateTime::parse_from_rfc3339("2024-05-01T05:46:13.508Z")
                .unwrap()
                .with_timezone(&Utc),
            price: 100.0,
            order_type: OrderType::Buy,
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
            ..Default::default()
        },
        // less than 12 hours later next trade 5/6/2024 8AM
        ExchangeOrder {
            user: 1,
            accepter: 2,
            filled_at: DateTime::parse_from_rfc3339("2024-05-06T08:01:00Z")
                .unwrap()
                .with_timezone(&Utc),
            amount: 40000.0,
            created_at: DateTime::parse_from_rfc3339("2024-05-01T05:46:13.508Z")
                .unwrap()
                .with_timezone(&Utc),
            price: 4.0,
            order_type: OrderType::Buy,
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
            ..Default::default()
        },
        // less than one hour later
        ExchangeOrder {
            user: 1,
            accepter: 2,
            filled_at: DateTime::parse_from_rfc3339("2024-05-06T09:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            amount: 40000.0,
            created_at: DateTime::parse_from_rfc3339("2024-05-01T05:46:13.508Z")
                .unwrap()
                .with_timezone(&Utc),
            price: 4.0,
            order_type: OrderType::Buy,
            rms_hour: 0.0,
            rms_24hour: 0.0,
            price_vs_rms_hour: 0.0,
            price_vs_rms_24hour: 0.0,
            ..Default::default()
        },
        // same time as previous but different traders
        ExchangeOrder {
            user: 300,     // carol
            accepter: 400, // dave
            filled_at: DateTime::parse_from_rfc3339("2024-05-06T09:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            amount: 25000.0,
            created_at: DateTime::parse_from_rfc3339("2024-05-01T03:46:13.508Z")
                .unwrap()
                .with_timezone(&Utc),
            price: 32.0,
            ..Default::default()
        },
    ];

    include_rms_stats(&mut swaps);

    let s0 = swaps.first().unwrap();
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

    process_shill(&mut swaps);
    dbg!(&swaps);
}

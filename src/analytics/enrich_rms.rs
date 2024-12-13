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

    for o in all_offers {
        if competition.offer_type != o.order_type {
            continue;
        }

        // is the offer
        if o.filled_at > current_order.filled_at && o.created_at <= current_order.filled_at {
            competition.open_same_type += 1;
            if o.amount <= current_order.amount {
                competition.within_amount += 1;

                if o.price <= current_order.price {
                    competition.within_amount_lower_price += 1;
                }
            }
        }
    }
    competition
}
pub fn process_shill(all_transactions: &mut [ExchangeOrder]) {
    all_transactions.sort_by_key(|el| el.filled_at); // Sort by filled_at

    for i in 0..all_transactions.len() {
        // TODO: gross, don't enumerate, borrow checker you won the battle
        let mut current_order = all_transactions[i].clone();
        let comp = get_competing_offers(&current_order, all_transactions);

        // We can only evaluate if an "accepter" is engaged in shill behavior.
        // the "offerer" may create unreasonable offers, but the shill trade requires someone accepting.

        match current_order.order_type {
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

// pub fn process_sell_order_shill(swaps: &mut [ExchangeOrder]) {
//     swaps.sort_by_key(|swap| swap.filled_at); // Sort by filled_at

//     // for i in 0..swaps.len() {
//     //     let current_swap = &swaps[i];
//     //     // TODO: move this to a filter on the enclosing scope
//     //     if current_swap.shill_bid.is_some() {
//     //         continue;
//     //     };

//     //     // Filter for open trades
//     //     let open_orders = swaps
//     //         .iter()
//     //         .filter(|&other_swap| {
//     //             other_swap.filled_at > current_swap.filled_at
//     //                 && other_swap.created_at <= current_swap.filled_at
//     //         })
//     //         .collect::<Vec<_>>();

//     //     // Determine if the current swap took the best price
//     //     let is_shill_bid = match current_swap.order_type.as_str() {
//     //         // Signs of shill trades.
//     //         // For those offering to SELL coins, as the tx.user (offerer)
//     //         // I should offer to sell near the current clearing price.
//     //         // If I'm making shill bids, I'm creating trades above the current clearing price. An honest actor wouldn't expect those to get filled immediately.
//     //         // If an accepter is buying coins at a higher price than other orders which could be filled, then they are likely colluding to increase the price.
//     //         "Sell" => open_orders.iter().any(|other_swap|
//     //               // if there are cheaper SELL offers,
//     //               // for smaller sizes, then the rational honest actor
//     //               // will pick one of those.
//     //               // So we find the list of open orders which would be
//     //               // better than the one taken how.
//     //               // if there are ANY available, then this SELL order was
//     //               // filled dishonestly.
//     //               other_swap.price <= current_swap.price &&
//     //               other_swap.amount <= current_swap.amount),
//     //         _ => false,
//     //     };

//     //     // Update the swap with the best price flag
//     //     swaps[i].shill_bid = Some(is_shill_bid);
//     // }
// }

// pub fn process_buy_order_shill(swaps: &mut [ExchangeOrder]) {
//     // NEED to sort by created_at to identify shill created BUY orders
//     swaps.sort_by_key(|swap| swap.created_at);

//     for i in 0..swaps.len() {
//         let current_swap = &swaps[i];

//         // TODO: move this to a filter on the enclosing scope
//         if current_swap.shill_bid.is_some() {
//             continue;
//         };

//         // // Filter for open trades
//         // let open_orders = swaps
//         //     .iter()
//         //     .filter(|&other_swap| {
//         //         other_swap.filled_at > current_swap.created_at
//         //             && other_swap.created_at <= current_swap.created_at
//         //     })
//         //     .collect::<Vec<_>>();

//         // // Determine if the current swap took the best price
//         // let is_shill_bid = match current_swap.order_type.as_str() {
//         //     // Signs of shill trades.
//         //     // For those offering to BUY coins, as the tx.user (offerer)
//         //     // An honest and rational actor would not create a buy order
//         //     // higher than other SELL offers which have not been filled.
//         //     // The shill bidder who is colluding will create a BUY order at a higher price than other SELL orders which currently exist.
//         //     "Buy" => open_orders.iter().any(|other_swap| {
//         //         if other_swap.order_type == *"Sell" {
//         //             // this is not a rational trade if there are
//         //             // SELL offers of the same amount (or smaller)
//         //             // at a price equal or lower.
//         //             return other_swap.price <= current_swap.price
//         //                 && other_swap.amount <= current_swap.amount;
//         //         }
//         //         false
//         //     }),
//         //     _ => false,
//         // };

//         // Update the swap with the best price flag
//         swaps[i].shill_bid = Some(false);
//     }
// }

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

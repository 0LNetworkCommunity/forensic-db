MATCH ()-[r:Swap {`accepter_shill_up`: true }]->(accepter)
// WHERE date(r.filled_at) > date("2024-02-10")
//  AND date(r.filled_at) < date("2024-03-02")
RETURN DISTINCT(accepter.`swap_id`) AS user , count(r) AS count, toFloat(sum(r.amount)) AS amount
ORDER BY amount DESC

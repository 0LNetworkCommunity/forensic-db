MATCH (a)-[r:Swap]-(b)
WITH a, b,
     count(r) AS total_count,
     sum(CASE WHEN r.`accepter_shill_up` = true THEN 1 ELSE 0 END) AS shill_bid_count,
     sum(CASE WHEN r.price_vs_rms_hour > 1 THEN 1 ELSE 0 END) AS price_vs_rms24h_count
WHERE total_count >= 5
ORDER BY total_count DESC
RETURN
    a,
    b,
    total_count,
    shill_bid_count,
    (toFloat(shill_bid_count) / total_count) * 100 AS shill_bid_percentage,
    price_vs_rms24h_count,
    (toFloat(price_vs_rms24h_count) / total_count) * 100 AS price_vs_rms24h_percentage

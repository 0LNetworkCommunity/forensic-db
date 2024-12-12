MATCH (a)-[r:Swap]-()

WITH a,
     count(r) AS total_trades,
     sum(CASE WHEN r.shill_bid = true THEN 1 ELSE 0 END) AS shill_bid_count,
    sum(CASE WHEN r.price_vs_rms_hour > 1.0 THEN 1 ELSE 0 END) AS pump_count
WHERE total_trades > 100
RETURN
    a,
    total_trades,
    shill_bid_count,
    (toFloat(shill_bid_count) / total_trades) AS shill_bid_percentage,
    (toFloat(pump_count) / total_trades) AS pump_percentage
ORDER BY shill_bid_percentage DESC

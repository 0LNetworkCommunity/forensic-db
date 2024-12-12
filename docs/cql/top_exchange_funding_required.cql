MATCH p=(e:SwapAccount)-[d:DailyLedger]-(ul:UserLedger)
WHERE d.date < datetime("2024-01-16")
WITH e.swap_id AS id, max(ul.`total_funded`) as funded

RETURN id, funded
ORDER BY funded DESCENDING

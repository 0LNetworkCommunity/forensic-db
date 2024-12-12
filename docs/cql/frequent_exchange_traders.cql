MATCH (from:SwapAccount)-[r:Swap]-(to:SwapAccount)
WITH from, to, COUNT(r) AS transaction_count
ORDER BY transaction_count DESC
LIMIT 500
MATCH p=(from)-[r:Swap]-(to)
RETURN p, transaction_count

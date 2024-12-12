MATCH p=()-[r:Swap {`shill_bid`: TRUE }]->()
WHERE date(r.filled_at) > date("2024-02-10")
AND date(r.filled_at) < date("2024-03-02")
RETURN p

MATCH (a)-[r:Swap]-(b)
WITH a, b, count(r) AS ab_count
WHERE ab_count >= 5
MATCH (a)-[r_all:Swap]-()
WITH a, b, ab_count, count(r_all) AS total_a_count
RETURN
    a,
    b,
    ab_count,
    total_a_count,
    (toFloat(ab_count) / total_a_count) * 100 AS exclusivity_percentage
ORDER BY ab_count DESC, exclusivity_percentage DESC
LIMIT 100

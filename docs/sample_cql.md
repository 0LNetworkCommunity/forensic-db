

# Cypher query to map shill trades

```
//Top 100 shill pairs
MATCH (from:SwapAccount)-[r:Swap {shill_bid: true}]->(to:SwapAccount)
WHERE r.price_vs_rms_hour > 1

// combine from and to as users
WITH DISTINCT(collect(DISTINCT from) + collect(DISTINCT to)) AS all_users, r
UNWIND all_users AS user

WITH user, COUNT(r) AS shill_bid_count
// sorts by user with the highest count of relations with shill_bid
ORDER BY shill_bid_count DESC
// get top 100
LIMIT 100

// find all paths of owners, to tx, to onramp account
// don't need to find all paths, just the shortest one
MATCH p=SHORTEST 1 ()-[o:Owns]->(:Account)-[t:Tx]-()-[:OnRamp]->(user)
// use regex to exclude certain functions
WHERE NOT t.function =~ '(?i).*vouch.*'
// or better
/ WHERE NONE(r IN relationships(p) WHERE r.function IS NOT NULL AND r.function =~ '(?i).*vouch.*' )

// show the paths
return p
```


# Find all known users and their exchange address
```
WITH ['0xf57d3968d0bfd5b3120fda88f34310c70bd72033f77422f4407fbbef7c24557a'] AS exclude

MATCH p = SHORTEST 1 (o:Owner)-[r *..3]->(:SwapAccount)
WHERE NONE(
  r IN relationships(p)
    WHERE r.function IS NOT NULL
    AND r.function =~ '(?i).*vouch.*'
  )
  AND NONE(
    n IN nodes(p)
    WHERE n.address IS NOT NULL
    AND n.address IN exclude
  )
RETURN p
```

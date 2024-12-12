MATCH p=SHORTEST 1 (a:SwapAccount)-[r:Swap]->(b:SwapAccount)-[r2:Swap]->(a:SwapAccount)
RETURN p

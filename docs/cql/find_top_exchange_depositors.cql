WITH "0xf57d3968d0bfd5b3120fda88f34310c70bd72033f77422f4407fbbef7c24557a" AS olswap_deposit

// Step 1: Get the list of all depositors
MATCH (depositor:Account)-[tx:Tx]->(onboard:Account {address: olswap_deposit})
WITH COLLECT(DISTINCT depositor) AS all_depositors, olswap_deposit, tx

// Step 2: Match depositors and amounts within the date range

UNWIND all_depositors AS depositor

OPTIONAL MATCH (depositor)-[tx2:Tx]->(onboard:Account {address: olswap_deposit})
WHERE tx2.block_datetime >= datetime('2024-01-07') AND tx2.block_datetime <= datetime('2024-01-09')


RETURN
  depositor.address AS depositor_address,
  COALESCE(SUM(tx2.V7_OlAccountTransfer_amount), 0) AS deposit_amount,
  count(tx2)
ORDER BY deposit_amount DESC

# forensic-db

An ETL system for processing Libra backup archives from genesis to present into a graph database.

Uses Open Cypher for compatibility with Neo4j, AWS Neptune, Memgraph, etc.

By default uses Neo4j as target database.


## Source Files
You will use Backup archives from https://github.com/0LNetworkCommunity/epoch-archive-mainnet

Note there are different Git branches for each of v5, v6, v7 archives.

## Build
```
cargo build release
cp ./target/libra-forensic-db ~/.cargo/bin

```

## Load chain archives

### NOTE you must clone the backup archive repo above.
You should also unzip all the files (NOTE future versions of forensic-db will gzip extract for you).

```
# for example get the v6 data

git clone https://github.com/0LNetworkCommunity/epoch-archive-mainnet  --depth 1  --branch v6
```

### You must have a running NEO4j instance
Export the DB credentials to environment variables, or pass them as arguments to the tool.

If you don't have a running neo4j, you can use the [Neo4j desktop](https://neo4j.com/download/) tool to create a locally hosted db.
If you're feeling lucky and want to use docker: https://neo4j.com/docs/operations-manual/current/docker/introduction

```
# to run docker and persist  data between restarts to a certain local directory
docker run \
    --restart always \
    --publish=7474:7474 --publish=7687:7687 \
    --env NEO4J_AUTH=none \
    --volume=/path/to/your/data:/data \
    neo4j:5.25.1-community
```

### Using credentials
```
# Use these arguments in your env.
export LIBRA_GRAPH_DB_URI='neo4j+s://example.databases.neo4j.io'
export LIBRA_GRAPH_DB_USER='neo4j'
export LIBRA_GRAPH_DB_PASS='your-password'

```

Or include in the command line arguments
```
libra-forensic-db --db-uri 'neo4j+s://example.databases.neo4j.io' --db-username 'neo4j' --db-password 'your-password' <sub-command e.g. ingest-all>

```

### ingest data
For example: ingest all archives for `transaction` records.


```
# change to the path where epoch-archive-mainnet repo is located
cd epoch-archive-mainnet

# to view detailed logs:
export RUST_LOG=info

# load all transactional backups from epoch archive
libra-forensic-db ingest-all --start-path <path to epoch-archive> --archive-content transaction

```

## Enrich data
You can add off-chain data to the forensic db. Currently, exchange transactions are supported from JSON with the following format:

```
[{"user":1,"orderType":"Sell","amount":"40000.000","price":"0.00460","created_at":"2024-05-12T15:25:14.991Z","filled_at":"2024-05-14T15:04:13.000Z","accepter":3768}]
```

#### enrich data

```
libra-forensic-db enrich --swap-record-json <path to .json file>
```

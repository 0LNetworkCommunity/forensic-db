# forensic-db

An ETL system for processing Libra backup archives from genesis to present into a graph database.

Uses Open Cypher for compatibility with Neo4j, AWS Neptune, Memgraph, etc.

By default uses Neo4j as target database.


## Source Files
You will use Backup archives from https://github.com/0LNetworkCommunity/epoch-archive-mainnet
Note there is a v5, v6, v7 branches of the archives.

## Build
```
cargo build release
cp ./target/libra-forsensic-db ~/.cargo/bin
 
```

## Run

### NOTE you must close the backup archive repo above.
You should also unzip all the files (NOTE future versions of forensic-db will gzip extract for you).

### You must have a running NEO4j instance
Export the DB credentials to environment variables, or pass them as arguments to the tool.



```
# load all transactional backups from epoch archive
libra-forensic-db ingest-all --start-from <path to epoch-archive> --content transactions
```
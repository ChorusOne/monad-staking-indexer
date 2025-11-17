# Monad staking indexer

Store all events from the [staking contract](https://docs.monad.xyz/developer-essentials/staking/staking-precompile) in a Postgres database. Storing all events requires about 5GiB per month.

In case this program is stopped, it will backfill missing blocks.

You need to ensure that the configured RPC has a sufficient block history available.

There's a prometheus metrics endpoint at `:9090/metrics`.

## Set up the database

Create the database and users:

```
$ psql -h localhost -p 5400 -f migrations/00_init_database.psql
```

## Run migrations

```
export PGUSER=monad_staking_setup
export PGPASSWORD=monad_staking_setup
export PGDATABASE=monad_staking
sqlx migrate run
```

## Connect to the db for exploration

```
psql -U monad_staking_app -h localhost -p 5400
```

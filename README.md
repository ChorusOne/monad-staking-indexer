# Monad staking indexer

Store all events from the [staking contract](https://docs.monad.xyz/developer-essentials/staking/staking-precompile) in a Postgres database. Storing all events requires about 5GiB per month.

In case this program is stopped, it will backfill missing blocks.

You need to ensure that the configured RPC has a sufficient block history available.

There's a prometheus metrics endpoint at `:9090/metrics`.

## Set up the database

Create the database and users:

```
PGPASSWORD=postgres psql -h 127.0.0.1 -p 5400 -U postgres -d postgres -f migrations/00_init_database.psql
```

## Run migrations

```
export DATABASE_URL=postgres://monad_staking_setup:monad_staking_setup@127.0.0.1:5400/monad_staking_indexer
sqlx migrate run
```

## Configure the indexer

The indexer does not read `DATABASE_URL` at runtime. `DATABASE_URL` is only used
by `sqlx migrate run`; the indexer itself reads `config.toml`.

For local development with the Docker Compose database, start from the example
config. It is already configured for the local database and app role created by
`migrations/00_init_database.psql`.

```
cp config.toml.example config.toml
```

Edit `config.toml` if you need a different RPC URL.

Then run the indexer:

```
cargo run
```

## Connect to the db for exploration

```
PGPASSWORD=monad_staking_app psql -h 127.0.0.1 -p 5400 -U monad_staking_app -d monad_staking_indexer
```

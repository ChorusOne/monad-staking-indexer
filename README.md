## Create the database

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
## Connect to the db

```
psql -U monad_staking_app -h localhost -p 5400
```

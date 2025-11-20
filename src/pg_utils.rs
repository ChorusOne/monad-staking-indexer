//! Utilities for running Postgres locally for testing and development.

use std::fs;
use std::path::Path;
use std::process::{Child, Command};

use scopeguard::defer;
use sqlx::PgPool;

use crate::error::{Error, Result, ResultExt};

/// Spawn a program through `setpriv` and set the parent death signal, to ensure
/// that the spawned process gets killed when the parent process (us) dies, so
/// you don't get lingering processes when you Ctrl+C your development environment.
pub fn new_command(binary: &str) -> Command {
    let mut cmd = {
        #[cfg(target_os = "linux")]
        {
            let mut cmd = Command::new("setpriv");
            cmd.args(["--pdeathsig", "KILL", "--"]);
            cmd.arg(binary);
            cmd
        }
        #[cfg(not(target_os = "linux"))]
        {
            Command::new(binary)
        }
    };

    let new_path = format!(
        "{}:/usr/lib/postgresql/16/bin",
        std::env::var("PATH").unwrap_or_default()
    );
    cmd.env("PATH", new_path);
    cmd
}

/// Run `initdb` in the given directory and start a Postgres there.
///
/// The database superuser will be `postgres` with password `postgres`. The
/// database is configured to only listen on a Unix domain socket, so there are
/// no issues with ports being in and multiple postgreses can be spawned in
/// parallel for use in tests. The socket will be in `dir`, so that directory
/// should be used as `PGHOST`.
pub fn start_postgres(dir: &Path) -> Result<Child> {
    fs::create_dir_all(dir).or_app_err("Failed to create Postgres dir.")?;

    let data_dir = dir.join("data");
    let pwfile = dir.join("password");

    // The database superuser will be "postgres" with password "postgres".
    fs::write(&pwfile, "postgres").or_app_err("Failed to write pwfile.")?;

    let db_exists = data_dir.exists();
    if !db_exists {
        let initdb_output = new_command("initdb")
            .arg("--pgdata")
            .arg(&data_dir)
            .args(["--encoding", "UTF-8"])
            .args(["--locale", "C"])
            .args(["--username", "postgres"])
            .arg("--pwfile")
            .arg(&pwfile)
            .env("TZ", "Etc/UTC")
            .output()
            .or_app_err("Failed to spawn `initdb`.")?;

        if !initdb_output.status.success() {
            std::io::Write::write_all(&mut std::io::stdout(), &initdb_output.stdout)
                .or_app_err("Failed to write initdb stdout")?;
            std::io::Write::write_all(&mut std::io::stderr(), &initdb_output.stderr)
                .or_app_err("Failed to write initdb stderr")?;
            return Err(Error::new("`initdb` failed."));
        }
    }

    let postgres = new_command("postgres")
        // For some reason, running from the current directory and passing the
        // data dir and socket dir results in a "no such file or directory", but
        // when we chdir into the data directory, it does work.
        .current_dir(&data_dir)
        .arg("-D")
        .arg(".")
        .arg("-k")
        .arg("..")
        // Set the listen address to empty string, we only want to listen on a Unix
        // socket, not on a port.
        .args(["-c", "listen_addresses="])
        .args(["-c", "timezone=UTC"])
        .spawn()
        .or_app_err("Failed to spawn `postgres`.")?;

    // When setting PGHOST to connect to our new database, we need an absolute
    // path, for some reason relative paths don't work.
    let canonical_dir =
        fs::canonicalize(dir).or_app_err("Failed to canonicalize Postgres directory.")?;

    // Wait up to 30 seconds for Postgres to become ready.
    let mut is_ready = false;
    for _ in 0..10 {
        let is_ready_status = new_command("pg_isready")
            .arg("--host")
            .arg(&canonical_dir)
            .arg("--timeout")
            .arg("3" /* seconds */)
            .arg("--username")
            .arg("postgres")
            // Explicitly select which database we are waiting for. We are *not*
            // waiting for the app-level user, because it may not have been created yet, we
            // are only waiting for the system catalog.
            .arg("--dbname")
            .arg("postgres")
            .status()
            .or_app_err("Failed to spawn `pg_isready`.")?;

        if is_ready_status.success() {
            is_ready = true;
            break;
        }

        // For some reason the above command does not always wait 3 seconds, so
        // wait some additional time here if Postgres is not yet ready.
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    if !is_ready {
        return Err(Error::new("Postgres did not become ready fast enough."));
    }

    Ok(postgres)
}

/// Start a Postgres instance with a temporary data directory.
///
/// When the Postgres is ready, calls the closure with `PGHOST` as argument. It
/// contains the absolute path to a Unix domain socket to connect to. The
/// database superuser is `postgres` with password `postgres`.
pub fn with_postgres<F: FnOnce(&str) -> Result<()>>(f: F) -> Result<()> {
    let dir =
        tempfile::tempdir().or_app_err("Failed to create temporary directory for Postgres.")?;
    let mut postgres = start_postgres(dir.path())?;

    defer! {
        let _ = postgres.kill();
    }

    f(dir
        .path()
        .as_os_str()
        .to_str()
        .expect("Temp dir path should be valid UTF-8."))
}

/// Create the users and database, run all migrations.
///
/// See also [`with_postgres_and_schema`].
pub fn execute_migrations(pg_host: &str, user: &str, db_name: &str) -> Result<()> {
    let psql_output = new_command("psql")
        .env("PGUSER", "postgres")
        .env("PGPASSWORD", "postgres")
        .env("PGHOST", pg_host)
        .env("PGDATABASE", "postgres")
        .arg("--file")
        .arg("migrations/00_init_database.psql")
        .output()
        .or_app_err("Failed to spawn `psql`.")?;

    if !psql_output.status.success() {
        std::io::Write::write_all(&mut std::io::stdout(), &psql_output.stdout)
            .or_app_err("Failed to write psql stdout")?;
        std::io::Write::write_all(&mut std::io::stderr(), &psql_output.stderr)
            .or_app_err("Failed to write psql stderr")?;
        return Err(Error::new("Failed to run `00_init_database.psql`."));
    }

    let sqlx_output = new_command("sqlx")
        .env("PGUSER", user)
        .env("PGPASSWORD", user)
        .env("PGHOST", pg_host)
        .args([
            "migrate",
            "run",
            "--database-url",
            &format!("postgres:///{db_name}"),
        ])
        .output()
        .or_app_err("Failed to spawn `sqlx`.")?;

    if !sqlx_output.status.success() {
        std::io::Write::write_all(&mut std::io::stdout(), &sqlx_output.stdout)
            .or_app_err("Failed to write sqlx stdout")?;
        std::io::Write::write_all(&mut std::io::stderr(), &sqlx_output.stderr)
            .or_app_err("Failed to write sqlx stderr")?;
        return Err(Error::new("Failed to run migrations with `sqlx`."));
    }

    Ok(())
}

/// Start a Postgres instance with a temporary data directory, run all migrations against it.
///
/// When the Postgres is ready and all migrations have been executed, calls the
/// closure with `PGHOST` as argument. It contains the absolute path to a Unix
/// domain socket to connect to. The `app` and `setup` users exist at
/// this point, passwords are equal to the usernames.
pub fn with_postgres_and_schema<F: FnOnce(&str) -> Result<()>>(f: F) -> Result<()> {
    with_postgres(|pg_host| {
        execute_migrations(pg_host, "monad_staking_setup", "monad_staking")?;
        f(pg_host)
    })
}

/// Start a Postgres instance with a temporary data directory, run all migrations against it.
///
/// Async version of [`with_postgres_and_schema`]. When the Postgres is ready and all migrations
/// have been executed, calls the async closure with a `PgPool` connection pool. The `app` and
/// `setup` users exist at this point, passwords are equal to the usernames.
pub fn with_postgres_and_schema_async<F, Fut>(f: F) -> Result<()>
where
    F: FnOnce(PgPool) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), Box<dyn std::error::Error>>>,
{
    let user = "monad_staking_setup";
    let db = "monad_staking_indexer";
    with_postgres(|pg_host| {
        execute_migrations(pg_host, user, db)?;
        let connection_url = format!("postgresql://{user}:{user}@localhost/{db}?host={pg_host}");
        let runtime =
            tokio::runtime::Runtime::new().or_app_err("Failed to create tokio runtime.")?;

        runtime
            .block_on(async {
                let pool = crate::db::create_pool(&connection_url)
                    .await
                    .map_err(|e| format!("Failed to create pool: {}", e))?;

                tokio::time::timeout(std::time::Duration::from_secs(5), f(pool))
                    .await
                    .map_err(|_| "Test timeout after 5 seconds".into())
                    .and_then(|result| result)
            })
            .map_err(|e| Error::new(format!("Async callback failed: {}", e)))
    })
}

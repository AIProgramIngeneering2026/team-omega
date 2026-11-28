import argparse
import sqlite3
from contextlib import closing
from pathlib import Path


_MIGRATIONS = Path(__file__).with_name("migrations")


def _execute_migration(connection: sqlite3.Connection, script: str) -> None:
    # executescript() would commit the open transaction before running the SQL.
    statement = ""
    for part in (script + "\n;").split(";")[:-1]:
        statement += part + ";"
        if sqlite3.complete_statement(statement):
            connection.execute(statement)
            statement = ""
    if statement.strip():
        raise ValueError("Миграция содержит незавершённую SQL-команду")


def migrate(database: str | Path) -> None:
    migrations = sorted(_MIGRATIONS.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    if not migrations:
        raise RuntimeError("SQL-миграции не найдены")

    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)"
        )
        applied = {row[0] for row in connection.execute("SELECT version FROM schema_migrations")}
        applying_script = False

        def authorize(action: int, *_: str | None) -> int:
            # Only the runner may commit or roll back its transaction.
            if applying_script and action == sqlite3.SQLITE_TRANSACTION:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorize)
        for path in migrations:
            version = int(path.name.split("_", 1)[0])
            if version not in applied:
                applying_script = True
                try:
                    _execute_migration(connection, path.read_text(encoding="utf-8"))
                finally:
                    applying_script = False
                connection.execute("INSERT INTO schema_migrations (version) VALUES (?)", (version,))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply Omega Rooms database migrations")
    parser.add_argument("database", type=Path, help="Path to the SQLite database file")
    migrate(parser.parse_args().database)

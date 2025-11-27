from dbapi_d1 import Connection
from dotenv import load_dotenv
import os
import sqlalchemy_d1  # IMPORTANT: This loads the dialect, do not remove
from sqlalchemy import create_engine, text, inspect

load_dotenv()

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
D1_DB_ID = os.environ.get("D1_DB_ID")

TEST_LEVELS = [
    1,  # simple SELECT 1
    2,  # single-row queries
    3,  # multi-row & aggregates
    4,  # reflection (tables/columns)
]

# IMPORTANT: FIRST RUN IN D1 DATABASE CONSOLE FOR TESTS TO PASS:
#
# CREATE TABLE IF NOT EXISTS test_table ( id INTEGER PRIMARY KEY, name TEXT NOT NULL, value INTEGER NOT NULL, active BOOLEAN DEFAULT 1 ); 
# INSERT INTO test_table (id, name, value, active) VALUES (1, 'Alice', 42, 1), (2, 'Bob', 55, 0); 
# 
# CREATE TABLE IF NOT EXISTS another_table ( id INTEGER PRIMARY KEY, description TEXT ); 
# INSERT INTO another_table (id, description) VALUES (1, 'Sample row for reflection');


class DBAPITests:
    def __init__(self, conn: Connection):
        self.conn = conn
        self.cur = conn.cursor()

    def lvl1(self):
        # Simple literal select
        self.cur.execute("SELECT 1 AS test_value")
        result = self.cur.fetchall()
        assert result == [(1,)], f"Expected [(1,)], got {result}"
        print("DBAPI lvl1 passed:", result)

    def lvl2(self):
        # Single row query with parameter binding
        self.cur.execute(
            "SELECT id, name, value FROM test_table WHERE id = ?", [1]
        )
        result = self.cur.fetchall()
        expected = [(1, "Alice", 42)]
        assert result == expected, f"Expected {expected}, got {result}"
        print("DBAPI lvl2 passed:", result)

    def lvl3(self):
        # Multi-row query
        self.cur.execute(
            "SELECT id, name, value FROM test_table WHERE value > ?", [10]
        )
        result = self.cur.fetchall()
        expected = [(1, "Alice", 42), (2, "Bob", 55)]
        assert result == expected, f"Expected {expected}, got {result}"
        print("DBAPI lvl3 multi-row passed:", result)

        # Aggregate query
        self.cur.execute("SELECT COUNT(*) FROM test_table")
        count = self.cur.fetchall()
        assert count == [(2,)], f"Expected [(2,)], got {count}"
        print("DBAPI lvl3 aggregate passed:", count)

    def lvl4(self):
        # Reflection: tables
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in self.cur.fetchall()]
        assert "test_table" in tables
        assert "another_table" in tables
        print("DBAPI lvl4 tables found:", tables)

        # Reflection: columns
        self.cur.execute("PRAGMA table_info('test_table')")
        columns = [c[1] for c in self.cur.fetchall()]
        expected_cols = ["id", "name", "value", "active"]
        for col in expected_cols:
            assert col in columns, f"{col} missing in test_table"
        print("DBAPI lvl4 columns:", columns)

    def run(self, levels):
        for lvl in levels:
            getattr(self, f"lvl{lvl}")()


class SQLAlchemyTests:
    def __init__(self, engine):
        self.engine = engine

    def lvl1(self):
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS test_value")).fetchall()
            assert result == [(1,)], f"Expected [(1,)], got {result}"
            print("SQLAlchemy lvl1 passed:", result)

    def lvl2(self):
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, name, value FROM test_table WHERE id = :id"),
                {"id": 1},
            ).fetchall()
            expected = [(1, "Alice", 42)]
            assert result == expected, f"Expected {expected}, got {result}"
            print("SQLAlchemy lvl2 passed:", result)

    def lvl3(self):
        with self.engine.connect() as conn:
            # Multi-row query
            result = conn.execute(
                text(
                    "SELECT id, name, value FROM test_table WHERE value > :val"
                ),
                {"val": 10},
            ).fetchall()
            expected = [(1, "Alice", 42), (2, "Bob", 55)]
            assert result == expected, f"Expected {expected}, got {result}"
            print("SQLAlchemy lvl3 multi-row passed:", result)

            # Aggregate query
            count = conn.execute(
                text("SELECT COUNT(*) FROM test_table")
            ).fetchall()
            assert count == [(2,)], f"Expected [(2,)], got {count}"
            print("SQLAlchemy lvl3 aggregate passed:", count)

    def lvl4(self):
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        assert "test_table" in tables
        assert "another_table" in tables
        print("SQLAlchemy lvl4 tables:", tables)

        columns = inspector.get_columns("test_table")
        col_names = [col["name"] for col in columns]
        expected_cols = ["id", "name", "value", "active"]
        for col in expected_cols:
            assert col in col_names, f"{col} missing in test_table"
        print("SQLAlchemy lvl4 columns:", col_names)

    def run(self, levels):
        for lvl in levels:
            getattr(self, f"lvl{lvl}")()


def setup_dbapi_conn():
    return Connection(CF_ACCOUNT_ID, CF_API_TOKEN, D1_DB_ID)


def setup_sqlalchemy_engine():
    return create_engine(f"d1://{CF_ACCOUNT_ID}:{CF_API_TOKEN}@{D1_DB_ID}")


def main():
    print("Running DBAPI tests...")
    dbapi_conn = setup_dbapi_conn()
    DBAPITests(dbapi_conn).run(TEST_LEVELS)
    dbapi_conn.close()

    print("\nRunning SQLAlchemy tests...")
    engine = setup_sqlalchemy_engine()
    SQLAlchemyTests(engine).run(TEST_LEVELS)

    print("\nAll tests passed!")


if __name__ == "__main__":
    main()

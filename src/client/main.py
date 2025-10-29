from dbapi_d1 import connect
from sqlalchemy import create_engine, text
import sqlalchemy_d1


def test_dbapi_direct():
    print("Testing DB-API driver directly...")
    conn = connect("d1://example-dsn")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 as test_value")
    result = cursor.fetchall()
    print("DB-API result:", result)
    conn.close()


def test_sqlalchemy():
    print("\nTesting SQLAlchemy dialect...")
    engine = create_engine("d1://example-dsn")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 as test_value"))
        print("SQLAlchemy result:", result.fetchall())


def main():
    test_dbapi_direct()
    test_sqlalchemy()


if __name__ == "__main__":
    main()

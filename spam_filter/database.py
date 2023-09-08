import duckdb


def create_database():
    conn = duckdb.connect("./database.duckdb")

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS messages (
        spam BOOLEAN,
        body VARCHAR
    );
    """
    )

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS models (
        path VARCHAR,
        status VARCHAR,
        true_positive_rate FLOAT,
        false_positive_rate FLOAT
    );
    """
    )

if __name__ == "__main__":
    create_database()
    conn = duckdb.connect("./database.duckdb")
    tables = conn.execute("SHOW TABLES").fetchall()
    print(tables)

import sqlite3
import pandas as pd

NUMBER_VOTES = 3
BATCH_SIZE = 10
MINIMUM_SAMPLES = 100


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)

    return conn


def sqlite_2_pandas(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT * FROM `{}`".format(table))
    return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])


def create_batch(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM api_project")
    return list(cur.fetchall())


def assign_project():
    pass


def pull_data():
    pass


if __name__ == "__main__":
    conn = create_connection('db.sqlite3')
    results = sqlite_2_pandas(conn, "api_project")
    print(results)

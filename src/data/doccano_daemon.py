import sqlite3
import pandas as pd

NUMBER_VOTES = 3
BATCH_SIZE = 10
MINIMUM_SAMPLES = 100

PROJECT_COLS = [
    "name",
    "description",
    "guideline",
    "created_at",
    "updated_at",
    "project_type",
    "randomize_document_order",
    "polymorphic_ctype_id",
    "collaborative_annotation"
]

PROJECT_2_USER_COLS = ["project_id", "user_id"]


class DB:

    def __init__(self, db_file):
        self.conn = self.create_connection(db_file)

    def create_connection(self, db_file):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file, check_same_thread=False)
        except Exception as e:
            print(e)

        return conn

    @property
    def connection(self):
        return self.conn

    def sqlite_2_pandas(self, table):
        try:
            cur = self.conn.cursor()
        except Exception as e:
            return e

        cur.execute("SELECT * FROM `{}`".format(table))
        return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

    def insert_sqlite(self, table, columns, values):
        sql = "INSERT INTO {}({}) VALUES({})".format(table, ",".join(columns), ",".join("?"*len(columns)))
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()
        return cur.lastrowid

    def update_table(self, table, update, where):
        set_string = ", ".join([f"{key} = {value}" for key, value in update.items()])
        where_string = " OR ".join([f"{key} = {value}" for key, value in where.items()])
        sql = f"UPDATE {table} SET {set_string} WHERE {where_string}"
        print(sql)
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        cur.close()
        return cur


db = DB('db.sqlite3')


def get_new_batch():
    # call active learning to get a batch
    pass


def get_incomplete_batch():
    batches_2_users = db.sqlite_2_pandas("api_project_users")
    assignment_counts = batches_2_users.groupby("project_id").count().reset_index()
    incomplete_batches = assignment_counts.loc[assignment_counts["user_id"] < NUMBER_VOTES, 'project_id'].values
    if len(incomplete_batches) > 0:
        return incomplete_batches[0]
    else:
        return -1
    #     incomplete_batch_id = max(db.sqlite_2_pandas("api_project")["id"].values)
    # return incomplete_batch_id


def assign_project(user_id, project_id):
    return db.insert_sqlite("api_project_users", columns=PROJECT_2_USER_COLS, values=[project_id, user_id])


def insert_new_project_db():
    return db.insert_sqlite("api_project", columns=PROJECT_COLS,
                            values=["x", "desc", "guideline", 0, 0, "DocumentClassification", 1, 15, 0])


def pull_data():
    pass


def setup_batch(user_id):
    batch_id = get_incomplete_batch()
    if batch_id == -1:
        pass
    else:
        pass
    assign_project(int(user_id), int(get_incomplete_batch()))

# if __name__ == "__main__":
#     batches = db.sqlite_2_pandas("api_project")
#     batches_2_users = db.sqlite_2_pandas("api_project_users")
#     samples = db.sqlite_2_pandas("api_document")
#     print(get_incomplete_batch(batches_2_users))

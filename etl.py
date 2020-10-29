import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads temporary tables from AWS S3 bucket into Redshift Data Warehouse cluster
    using the COPY queries in `copy_table_queries` list.

    :param cur: cursor object
        Allows Python code to execute PostgreSQL command in a database session
        https://www.psycopg.org/docs/cursor.html
    :param conn: connection object
        Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
        https://www.psycopg.org/docs/connection.html
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data of final (fact and dimension) tables
    into analytics tables, from Data Warehouse (on Redshift)
    using the INSERT INTO queries in `insert_table_queries` list.

    :param cur: cursor object
        Allows Python code to execute PostgreSQL command in a database session
        https://www.psycopg.org/docs/cursor.html
    :param conn: connection object
        Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
        https://www.psycopg.org/docs/connection.html
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

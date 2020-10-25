import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.

    :param cur: cursor object
        Allows Python code to execute PostgreSQL command in a database session
        https://www.psycopg.org/docs/cursor.html
    :param conn: connection object
        Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
        https://www.psycopg.org/docs/connection.html
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.

    :param cur: cursor object
        Allows Python code to execute PostgreSQL command in a database session
        https://www.psycopg.org/docs/cursor.html
    :param conn: connection object
        Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
        https://www.psycopg.org/docs/connection.html
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with sparkify database and gets cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes connection.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # precondition: AWS Redshift Cluster must be launched
    # find this in AWS Redshift Cluster Properties
    # host comes from Endpoint, ends in amazonaws.com, and is the only one surrounded in single-quotes
    # ARN is also found in AWS Redshift Cluster Properties
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # conn.set_session(autocommit=True)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
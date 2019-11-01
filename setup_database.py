import bz2
import getpass
import os
from subprocess import PIPE, Popen

import psycopg2

import config
from ggdrive import download_operator
from sql_queries import create_table_queries, drop_table_queries


def create_user():
    sudo_password = getpass.getpass("sudo pwd: ")
    command = """
            sudo -u postgres psql -c "DROP USER vandl_dev;"
            sudo -u postgres psql -c "CREATE USER vandl_dev PASSWORD 'cryptocean';"
            sudo -u postgres psql -c "ALTER USER vandl_dev WITH SUPERUSER;"
            """.split()

    p = Popen(['sudo', '-S'] + command, stdin=PIPE,
              stderr=PIPE, universal_newlines=True)
    p.communicate(sudo_password + '\n')[1]


def create_database():
    create_user()
    """
    Create vietnam_stock database
    need to create superuser role ahead
    """
    # Connect to default database with pre-setup user
    conn = psycopg2.connect(
        f"host = 127.0.0.1 dbname = postgres user = {config.db_user} password = {config.db_pwd}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Stop activities on target db
    cur.execute(
        """SELECT * FROM pg_stat_activity WHERE datname = 'vietnam_stock';
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = 'vietnam_stock';""")

    # Recreate database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS vietnam_stock")
    cur.execute(
        "CREATE DATABASE vietnam_stock WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    cur.close()

    # Connect to vietnam_stock database
    conn = psycopg2.connect(config.conn_string)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    return cur, conn


def drop_tables(cur):
    for query in drop_table_queries:
        cur.execute(query)


def create_tables(cur):
    for query in create_table_queries:
        cur.execute(query)


def get_data(file_ids, patterns):
    # Download zip files from Google Drive
    download_operator(file_ids, patterns)

    # Uncompress data
    for pattern in patterns:
        pattern = f'cryptocean_{pattern}'
        file_path = f'./data/{pattern}.csv.bz2'
        zipfile = bz2.BZ2File(file_path)
        data = zipfile.read()
        new_file_path = file_path[:-4]  # Assuming the filepath ends with .bz2
        open(new_file_path, 'wb').write(data)  # Write an uncompressed file
        os.remove(file_path)


def initial_load(file_path, table, cur):
    try:
        f = open(file_path, 'r')
        # Load table from the file with header
        print(f'>>> Loading initial {table} data')
        cur.copy_expert(
            "copy {} from STDIN CSV HEADER QUOTE '\"'".format(table), f)
        cur.execute("commit;")
        print(f'>>> Loaded data into {table}')

    except Exception as e:
        print(getattr(e, "message", repr(e)))


def main():
    # Create new database and tables
    cur, conn = create_database()
    drop_tables(cur)
    create_tables(cur)

    # Prepare data for initial loads
    get_data(['1rhM3tn39cq9e0K-x8jCw-xXaFCGFHu73',
              '1TYYbKMc3b6b8gGtkD-0827HTDU-FWqcT'],
             ['historical_price', 'ticker'])

    # Initial load for ticker table
    initial_load("./data/cryptocean_ticker.csv",
                 'ticker',
                 cur)

    # Initial load for historical_price table
    initial_load(
        './data/cryptocean_historical_price.csv',
        'historical_price',
        cur)

    conn.close()


if __name__ == "__main__":
    main()

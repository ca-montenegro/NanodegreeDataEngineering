import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, song_select
from tqdm import tqdm
    
def load_staging_tables(cur, conn):
    """
    Function that load the staging tables into RedShift cluster
    
    Params:
    cur (cusor): Cursor to perform SQL operations
    conn (connection): Connection to the DB
    """
    print('Loading staging tables. Please wait, can take some minutes')
    for query in tqdm(copy_table_queries):
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Function that insert all data into RedShift cluster tables
    
    Params:
    cur (cusor): Cursor to perform SQL operations
    conn (connection): Connection to the DB
    """
    print('Inserting tables')
    for query in tqdm(insert_table_queries[3:]):
        cur.execute(query)
        conn.commit()


def main():
    """
    Main method of the script. Create the connection to the DB and the cursor, reading the config file.
    
    Calls the specified functions to load song_data and log_data.
    
    Finally, the connection to the DB is close.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
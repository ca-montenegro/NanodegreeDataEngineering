import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    
    print('Droping tables')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('Done')


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    
    print('Creating tables')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('Done')


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """    
    
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
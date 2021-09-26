import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' 
    ETL to load the staging tables
    
    cur: Cursor
    conn: Connection information to redshift using credentials
    queries as in sql_queries.py file       
    '''    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    ''' 
    ETL to insert data in the tables
    
    cur: Cursor
    conn: Connection information to redshift using credentials
    queries as in sql_queries.py file       
    '''    
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
#!/usr/bin/python
import psycopg2
from configparser import ConfigParser, NoSectionError

def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    if not parser.has_section(section):
        raise NoSectionError('Section {0} not found in the {1} file'.format(section, filename))

    params = parser.items(section)
    return {param[0]: param[1] for param in params}

def connect():
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        return psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        raise error


def run_single_query(query='SELECT version()'):
    conn = None
    try:
        conn = connect()
        cur = conn.cursor()
        
        # print('query: ',query)
        cur.execute(query)

        conn.commit()
        cur.close()
        print('Query successful')


    except (Exception, psycopg2.DatabaseError) as error:
        print('Query Error:')
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            
if __name__ == '__main__':
    result = run_single_query()
    print(result)
#!/usr/bin/python
import psycopg2
import json
from configparser import ConfigParser, NoSectionError
from datetime import date, datetime


def config(filename='./../database.ini', section='postgresql'):
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


def fetch_one(conn, query='SELECT version()' ):
    cur = conn.cursor()
    cur.execute(query)
    
    data = cur.fetchall()
    cur.close()
    print('Query successful')
    result =[]
    colnames = [desc[0] for desc in cur.description]
    result.extend(dict(zip(colnames, row)) for row in data)

    return result

if __name__ == '__main__':
    result = fetch_one()
    print(result)
#DB Connection Tools

import pymysql

def connect_sql(database = 'goodinfo'):
    conn = pymysql.connect(
            user='lailai',
            password='lailai',
            host='34.81.191.74',
            port=3306,
            charset='utf8mb4',
            database=database)
    cur = conn.cursor()
    return conn, cur

def connect_sql_stock(database = 'stock'):
    conn = pymysql.connect(
            user='stock',
            password='jIo23#dFhS@,e',
            host='34.81.191.74',
            port=3306,
            charset='utf8mb4',
            database=database)
    cur = conn.cursor()
    return conn, cur
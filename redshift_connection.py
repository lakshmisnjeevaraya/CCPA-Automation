#############################################################
#  Purpose: Contains all the Redshift DB connections
#  Author: Lakshmi.Sanjeevaraya
#############################################################

from psycopg2 import sql, connect
from setup import super_logger
from setup import logger
import mysql.connector
from mysql.connector import Error
import pymysql

# Redshift connection
def connection(dbname, dbuser, host, dbpassword, port):
    print("Hello")

    # create a global string for the PostgreSQL db name
    try:
        # declare a new PostgreSQL connection object
        conn = connect(
            dbname=dbname,
            user=dbuser,
            host=host,
            password=dbpassword,
            port=port,
         # sslmode = 'require'

        )
        print(conn)
        logger.info(f' Redshift Connection established Successfully {conn}')
        # data = col_cursor.fetchone()
        return conn
    except Exception as err:
        logger.error(f' psycopg2 connect() ERROR:{err}')


def mysqlconnection(myhost,myuser,mypassword):
    # myhost=
    # myuser=
    # mypassword


    try:
        connection = mysql.connector.connect(host=myhost,
                                             database='slingit_portal_rwdb',
                                             user=myuser,
                                             password=mypassword)

        # if connection.is_connected():
        #     db_Info = connection.get_server_info()
        #     print("Connected to MySQL Server version ", db_Info)
        #     cursor = connection.cursor()
        #     cursor.execute("select database();")
        #     record = cursor.fetchone()
        #     print("You're connected to database: ", record)
        return connection

    except Error as e:
        logger.error(f'Error while connecting to MySQL:{e}')

    # finally:
    #     if (connection.is_connected()):
    #         cursor.close()
    #         connection.close()
    #         print("MySQL connection is closed")


def mysqlconnection(myhost,myuser,mypassword):
    conn = pymysql.connect(host='cpacdb003.sling.com', port=3306, user="lakshmi", passwd="Val1dat3U53r")

    cur = conn.cursor()
    cur.execute("SELECT * FROM slingit_portal_rwdb.member WHERE email_address = 'BCASTILLO73@YAHOO.COM'")


    print(cur.description)
    print("Successfull connected")

    for row in cur:
        print(row)





    cur.close()
    conn.close()

if __name__ == '__main__':
    connection('dbname', 'dbuser', 'host', 'dbpassword', 'port', 'query')
    # mysqlconnection('myhost','myuser','mypassword')
    mysqlconnection('myhost','myuser','mypassword')

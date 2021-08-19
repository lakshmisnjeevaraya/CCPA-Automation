#############################################################
#  Purpose: Builds all the redshift Select and Unload query
#  Author: Lakshmi.Sanjeevaraya
#############################################################

import time
from setup import super_logger
from setup import logger
from google.oauth2 import service_account
from google.cloud import bigquery
from mysql.connector import Error


# Query to Select
def selectquery(unloadque, tab, id, date, redshiftconn):
    try:

        timezone = str("'" + 'US/Mountain' + "'")
        selectquery = unloadque.format(
            timezone, timezone
        ) + ' ' + tab + '\n' + "WHERE viewerid =" + "'" + id + "'" + '\n' + "and" + ' ' + "FIRST_CURRENT_TIMESTAMP" + ' ' + ">=" + ' ' + "DateADD" + ' ' + '(' + "Day,-365," + "'" + date + ' ' + '00:00:00' + "'" + ')' + '\n' + "and" + ' ' + "FIRST_CURRENT_TIMESTAMP" + ' ' + "<=" + ' ' + "'" + date + ' ' + '00:00:00' + "'" + ' ' + "ORDER BY FIRST_CURRENT_TIMESTAMP)" + '\n' + "where rnk=" + "'" + str(
            1) + "'"
        logger.error(f'select query is:{selectquery}')
        cur = redshiftconn.cursor()

        cur.execute(selectquery)
        coldata = (cur.fetchall())
        logger.info(f'col_names is:{coldata}')

        return coldata
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')

        cursor = None


# Query to Select
def query_redshift(query, tab, id, date, redshiftconn, group, reporting_date):
    try:

        selectquery = query.format(tab, id)
        logger.error(f'query is:{selectquery}')

        # cur = conn.cursor()
        # cur.execute(selectquery)
        # coldata = cur.fetchone()

        cur = redshiftconn.cursor()
        cur.execute(selectquery)
        coldata = cur.fetchone()
        print("rowcountrowcountrowcountrowcount", coldata)
        logger.info(f'col_names is:{coldata}')

        return coldata
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{date}')
        super_logger.info(f'error_description:{err}')


# Query to Select
def delete_redshift(query, tab, id, date, redshiftconn, group, reporting_date):
    try:

        deletequery = query.format(tab, id)
        logger.error(f'query is:{deletequery}')

        cur = redshiftconn.cursor()
        cur.execute(deletequery)
        rowcount = cur.rowcount
        redshiftconn.commit()
        print("rowcountrowcountrowcountrowcount", rowcount)
        logger.info(f'col_names is:{rowcount}')

        return rowcount
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{date}')
        super_logger.info(f'error_description:{err}')


# Query to Select
def select_query_tab(query, tab, id, date, conn, group, reporting_date):
    try:

        selectquery = query.format(tab, id)
        logger.error(f'select query is:{selectquery}')

        cur = conn
        cur.execute(selectquery)
        coldata = cur.fetchone()
        logger.info(f'col_names is:{coldata}')

        return coldata
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{date}')
        super_logger.info(f'error_description:{err}')


# Query to Select
def select_query(unloadque, tab, id, date, redshiftconn, reporting_date):
    try:

        timezone = str("'" + 'US/Mountain' + "'")
        selectquery = unloadque.format(
            timezone, timezone
        ) + ' ' + tab + '\n' + "WHERE VIEWERID =" + "'" + id + "'" + '\n' + "and" + ' ' + "START_TIMESTAMP" + ' ' + ">=" + ' ' + "DateADD" + ' ' + '(' + "Day,-365," + "'" + date + ' ' + '00:00:00' + "'" + ')' + '\n' + "and" + ' ' + "START_TIMESTAMP" + ' ' + "<=" + ' ' + "'" + date + ' ' + '00:00:00' + "'" + ' ' + "ORDER BY START_TIMESTAMP)" + '\n' + "where rnk=" + "'" + str(
            1) + "'"
        logger.error(f'select query is:{selectquery}')
        cur = redshiftconn.cursor()
        cur.execute(selectquery)
        coldata = (cur.fetchall())
        logger.info(f'col_names is:{coldata}')

        return coldata
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{date}')
        super_logger.info(f'error_description:{err}')
        exit()

        cursor = None


# Query to Select slingbox
def selectslingboxquery(selectquery, tab, eamilid, redshiftconn, group,
                        reporting_date):
    try:

        selectquery = selectquery + ' ' + tab + '\n' + "WHERE email_address = " + "'" + eamilid + "'" + ';'
        print("selectqueryselectqueryselectqueryselectquery", selectquery)
        logger.error(f'select query is:{selectquery}')
        cur = redshiftconn.cursor()
        cur.execute(selectquery)

        coldata = (cur.fetchall())
        logger.info(f'col_names is:{coldata}')

        return coldata
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{date}')
        super_logger.info(f'error_description:{err}')
        exit()


# Query to Delete slingbox from mysql
def selectslingquery(selectquery, tab, eamilid, redshiftconn, submissiondate,
                     group, reporting_date):
    try:

        selectquery = selectquery + ' ' + tab + '\n' + "WHERE email_address = " + "'" + eamilid + "'" + ';'
        print(selectquery)
        logger.info(f'select query is:{selectquery}')
        cur = redshiftconn.cursor()
        cur.execute(selectquery)
        coldata = (cur.fetchall())
        print(coldata)

        logger.info(f'col_names is:{coldata}')
        return coldata
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')


# Query to Delete slingbox from mysql
def deleteslingboxquery(deletequery, tab, eamilid, mysqlconn, submissiondate,
                        group, reporting_date):
    try:

        deletequery = deletequery + ' ' + tab + '\n' + "WHERE email_address = " + "'" + eamilid + "'" + ';'
        print("deletequerydeletequery", deletequery)
        logger.error(f'select query is:{deletequery}')
        cur = mysqlconn.cursor()
        cur.execute(deletequery)

        mysqlconn.commit()
        return cur
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')

        cursor = None


# Query to Unload
def unload_query(tab, id, date, query, task, ccpaguid, role, redshiftconn,
                 reporting_date):
    try:
        timezone = str("\\" + "'" + 'US/Mountain' + "\\" + "'")
        unloadquery = "unload" + '(' + "'" + query.format(
            timezone, timezone
        ) + ' ' + tab + '\n' + "WHERE viewerid =" + "\\'" + id + "\\'" + '\n' + "and" + ' ' + "START_TIMESTAMP" + ' ' + ">=" + ' ' + "DateADD" + ' ' + '(' + "Day,-365," + "\\'" + date + ' ' + '00:00:00' + "\\'" + ')' + '\n' + "and" + ' ' + "START_TIMESTAMP" + ' ' + "<=" + ' ' + "\\'" + date + ' ' + '00:00:00' + "\\'" + ' ' + "ORDER BY START_TIMESTAMP)" + '\n' + "where rnk=" + "\\'" + str(
            1
        ) + "\\'" + "'" + ')' + '\n' + "to 's3://ccpa-request/" + ccpaguid + "_" + task + "_" + "FCC" + '.csv' + "'" + '\n' + role
        # Need to change this s3 location s3://ccpa-request/ccpa/test/
        logger.info(f'unload query:{unloadquery}')
        logger.info(f'Exporting data to datastoring_path')
        cur = redshiftconn.cursor()
        unload = cur.execute(unloadquery)
        time.sleep(60)
        redshiftconn.commit()
        return unload

    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:')
        super_logger.info(f'submission_date:{date}')
        super_logger.info(f'error_description:{err}')
        exit()

        cursor = None


# Query to UnloadSlingbox
def unload_query_sling(tab, emailId, date, query, task, ccpaguid, role,
                       redshiftconn, group, reporting_date):
    try:
        with open('unloadslingbox', 'r') as inserts:
            unloadquery_sling = inserts.read()
            unloadquery = "unload" + '(' + "'" + unloadquery_sling.format(
                date, date, date, date, date, date, date, date, date, date
            ) + ' ' + tab + '\n' + "WHERE email_address =" + "\\'" + emailId + "\\'" + ')' + '\n' + "where rnk=" + "\\'" + str(
                1
            ) + "\\'" + "'" + ')' + '\n' + "to 's3://ccpa-request/" + ccpaguid + "_" + task + "_" + "FSB" + '.csv' + "'" + '\n' + role
            # Need to change this s3 location s3://ccpa-request/ccpa/test/
            print('*********************************************')
            print("unloadqueryunloadqueryunloadquery", unloadquery)
            logger.info(f'unload query:{unloadquery}')
            logger.info(f'Exporting data to datastoring_path')
            cur = redshiftconn.cursor()
            unload = cur.execute(unloadquery)
            time.sleep(60)
            redshiftconn.commit()
            return unload

    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{date}')
        super_logger.info(f'error_description:{err}')


# Query to UnloadSlingbox
def dany_athena_query(accountid, date, athenaconn, group, reporting_date):
    try:
        with open('dany_query', 'r') as inserts:
            unloadquery_sling = inserts.read()
            unloadquery = unloadquery_sling.format(
                accountid, date, date
            ) + '\n' + "where account_id=" + "'" + accountid + "'" + '\n' + "and CAST(FIRST_CURRENT_TIMESTAMP as timestamp) >=" + "CAST(" + "'" + date + ' ' + "00:00:00" + "'" + ' ' + "as timestamp) - interval '365' day" + '\n' + "and CAST(FIRST_CURRENT_TIMESTAMP as timestamp) <=" + "CAST(" + "'" + date + ' ' + "00:00:00" + "'" + ' ' + "as timestamp) ORDER BY FIRST_CURRENT_TIMESTAMP"

            print('*********************************************')
            print("unloadqueryunloadqueryunloadquery", unloadquery)
            logger.info(f'unload query:{unloadquery}')
            logger.info(f'Exporting data to datastoring_path')
            cur = athenaconn
            cur.execute(unloadquery)
            time.sleep(60)
            return cur

    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{date}')
        super_logger.info(f'error_description:{err}')


# Query to UnloadSlingbox
def conviva_athena_query(query, tablename, ViwershipID, submissiondate,
                         athenaconn, reporting_date, group):
    try:

        unloadquery = query + ' ' + tablename + '\n' + "where VIEWERID=" + "'" + ViwershipID + "'" + '\n' + "and CAST(START_TIMESTAMP as timestamp) >=" + "CAST(" + "'" + submissiondate + ' ' + "00:00:00" + "'" + ' ' + "as timestamp) - interval '365' day" + '\n' + "and CAST(START_TIMESTAMP as timestamp) <=" + "CAST(" + "'" + submissiondate + ' ' + "00:00:00" + "'" + ' ' + "as timestamp) ORDER BY START_TIMESTAMP)" + '\n' + "where rnk=" + str(
            1)

        print('*********************************************')
        print("unloadqueryunloadqueryunloadquery", unloadquery)
        logger.info(f'unload query:{unloadquery}')
        logger.info(f'Exporting data to datastoring_path')
        cur = athenaconn
        cur.execute(unloadquery)
        print("####################################", cur.fetchall)
        time.sleep(60)
        return cur

    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'execution_status:FAIL')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')


def big_query(credentials, project_id):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials)
        client = bigquery.Client(credentials=credentials, project=project_id)

        # job = client.query(query)
        # result = job.result()
        # for row in result:
        #     print(row)

        return client
    except Error as e:
        print("Test")


# Query to Select withput reporting date
def selectquery_bq(query, conn):
    try:
        selectquery = query + ';'
        logger.info(f'query is:{selectquery}')
        job = conn.query(selectquery)
        result = job.result()
        for row in result:
            logger.info(f'Data is:{row}')
            return row[0]
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f'Execution ERROR:{err}')


# Query to Select withput reporting date
def deletequery_bq(query, conn):
    try:
        deletequery = query + ';'
        logger.info(f'query is:{deletequery}')
        job = conn.query(deletequery)
        result = job.result()
        for row in result:
            logger.info(f'Data is:{row}')
            return row[0]
    except Exception as err:
        logger.error(f'Execution ERROR:{err}')
        super_logger.info(f'Execution ERROR:{err}')


if __name__ == '__main__':
    selectquery("unloadque", "tab", "id", "date", "conn")
    unload_query("tab", id, "date", "query", "task", "ccpaguid", "role",
                 "redshiftconn")
    selectslingquery('selectquery', 'tab', 'eamilid', 'conn')

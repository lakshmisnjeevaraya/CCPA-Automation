#############################################################
#  Purpose: connect to Service Now (Snow) and pull open DA tasks for consumer protection
#           then get Mongo API details (account_id/dishCustomerId) for said request
#           then call each database and insert the records that start our CPA processes
#  Author: Lakshmi Sanjeevaraya
#############################################################

import redshift_connection
import configparser
import dateutil.parser
import build_query
import http_client
import os
import random
import string

from setup import logger
from setup import super_logger

from datetime import date

config = configparser.ConfigParser()
config.read('resource.ini')

DBconfig = configparser.ConfigParser()
DBconfig.read('config.ini')

base_url = config.get('SNOW API-Dev2', 'base_url')
assignment_id = config.get('SNOW API-Dev2', 'assignment_id_slingbox')

query = config.get('Query-Fields', 'query_delete').format(assignment_id)
Fields = config.get('Query-Fields', 'fields')
CustomTool = 'CPA-Slingbox'
user = os.environ.get("sling_api_prod_user")
passw = os.environ.get("sling_api_prod_pwd")

mongoDB_uri = config.get('Mongo-API', 'mongourl')
tablename = config.get('mysql-table', 'sling_table')

selectquery = config.get('query', 'selectquery')

# mysql credentials
myuser = os.environ.get("mysql_user")
myhost = DBconfig['mysql']['host']
mypassword = os.environ.get("mysql_password")
myport = DBconfig['mysql']['port']

# dblogger=setup.DBlogfile()
bq_project = config.get('bq', 'project_id')
dataset = config.get('bq', 'project_id')
bqtable = config.get('bq', 'project_id')

# mysql connection
mysqlconn = redshift_connection.mysqlconnection(myhost, myuser, mypassword)
print("mysqlconnmysqlconn", mysqlconn)

group = config.get('bq_table', 'slingbox_delete_group')

# Hit SNOW API and get the Tasks and consumer info

# Decode the JSON response into a dictionary and use the data
# for each request ID that is still open, first get Mongo details (account/dishCustomerId)
# then connect to each database and insert records to Oracle to start our search processes.
# cpa_task = records['number']
# cpa_record_sys_id = records['sys_id']


def ccpa_slingbox_delete():
    reporting_date = date.today()

    tasklist = http_client.get_snow_api(base_url, query, assignment_id, Fields,
                                        CustomTool, user, passw, group,
                                        reporting_date)
    totaltask = len(tasklist['result'])
    logger.info(f'Total number tasks: {totaltask}')

    for records in tasklist['result']:
        transaction_id = ''.join(random.choices(string.digits, k=5))
        super_logger.info(f'transction_id:{transaction_id}')
        super_logger.info(f'request_type:DeleteTask')

        super_logger.info(f'RequestType:DeleteTask')
        logger.info(f'Iterate Each task REcords values are :{records}')
        try:

            # strip off GUID/MongoID/RequestID from response.  It may be missing, so handle in try/catch
            task = records['number']
            super_logger.info(f'task_id:{task}')
            cpa_guid = records['parent.guid']
            super_logger.info(f'request_id:{cpa_guid}')
            sys_id = records['sys_id']

            keyobject = cpa_guid + '_' + task

            try:

                if len(cpa_guid) == 24:
                    # call Mongo API enpoint and capture response. This is an array
                    mongoresult = http_client.get_mongo_api(
                        mongoDB_uri, cpa_guid, CustomTool, task,
                        os.environ.get("mongo_user"), group, reporting_date)

                    # if mongoresult != {}:

                    for records2 in mongoresult['consumerPiiInfoList']:
                        if "email" in records2['consumer']:
                            emailId = records2['consumer']['email'][0]
                            try:

                                subdate = records2['request']['submissionDate']
                                submissiondate = str(
                                    dateutil.parser.parse(subdate).date())
                                logger.info(
                                    f'submissiondate :{submissiondate}')
                                if emailId != "":
                                    query_to_chk_data = build_query.selectslingquery(
                                        selectquery, tablename, emailId,
                                        mysqlconn, submissiondate, group,
                                        reporting_date)

                                    logger.info(
                                        f'data presenet :{query_to_chk_data}')
                                    super_logger.info(f's3_file_name:')

                                    # Validate data present for ViwershipID
                                    if query_to_chk_data != []:
                                        logger.info(
                                            f'Data present for this:{keyobject}'
                                        )
                                        # DElete the data from mysql:
                                        delete_data = build_query.deleteslingboxquery(
                                            config.get('query', 'deletequery'),
                                            tablename, emailId, mysqlconn,
                                            submissiondate, group,
                                            reporting_date)
                                        logger.info(
                                            f'Deleted the data from member table:{keyobject}'
                                        )
                                        closetask = http_client.delete_task(
                                            config.get('DB-Queries',
                                                       'deleteuri'), sys_id,
                                            keyobject, user, passw,
                                            config.get(
                                                'State-Closurecode-Delete',
                                                'state'),
                                            config.get(
                                                'State-Closurecode-Delete',
                                                'Data Deleted'),
                                            submissiondate, group,
                                            reporting_date)
                                        # When data for ViwershipID is not present
                                    else:
                                        super_logger.info(f's3_file_name:')

                                        closetask = http_client.delete_task(
                                            config.get('DB-Queries',
                                                       'deleteuri'), sys_id,
                                            keyobject, user, passw,
                                            config.get(
                                                'State-Closurecode-Delete',
                                                'state'),
                                            config.get(
                                                'State-Closurecode-Delete',
                                                'Data Deleted'),
                                            submissiondate, group,
                                            reporting_date)
                                        logger.info(
                                            f'No Data present for this task ID:{query_to_chk_data}'
                                        )

                                        logger.info(
                                            f'**************************No data present for this task: {keyobject} ******************************'
                                        )

                                else:
                                    messahe = 'Email id is Blank'
                                    logger.info(
                                        f'Email ID is Blank for this task:{keyobject}'
                                    )
                                    super_logger.info(f's3_file_name:')
                                    super_logger.info(f'execution_status:FAIL')
                                    super_logger.info(f'close_status_state:')
                                    super_logger.info(f'close_status_code:')
                                    super_logger.info(
                                        f'execution_date:{reporting_date}')
                                    super_logger.info(f'ccpa_group:{group}')
                                    super_logger.info(f'submission_date:')
                                    super_logger.info(
                                        f'error_description:{messahe}')

                            except Exception as err:
                                super_logger.info(f's3_file_name:')
                                super_logger.info(f'execution_status:FAIL')
                                super_logger.info(f'close_status_state:')
                                super_logger.info(f'close_status_code:')
                                super_logger.info(
                                    f'execution_date:{reporting_date}')
                                super_logger.info(f'ccpa_group:{group}')
                                super_logger.info(f'submission_date:')
                                super_logger.info(f'error_description:{err}')

                                logger.info(
                                    f'Error occurred in consumer info from Mongo API to get accountID/DciD:{err}'
                                )
                                logger.info(
                                    f'**************************Error occurred in consumer info from Mongo API for this task: {keyobject} ******************************'
                                )

                        else:
                            mess = 'Email section is not present'
                            super_logger.info(f's3_file_name:')
                            super_logger.info(f'execution_status:FAIL')
                            super_logger.info(f'close_status_state:')
                            super_logger.info(f'close_status_code:')
                            super_logger.info(
                                f'execution_date:{reporting_date}')
                            super_logger.info(f'ccpa_group:{group}')
                            super_logger.info(f'submission_date:')
                            super_logger.info(f'error_description:{mess}')

                            logger.error(
                                f'Email ID section is not present : {keyobject}'
                            )
                            logger.info(
                                f'**************************RequestID:{cpa_guid} is not valid for task:{task}******************************'
                            )

                elif len(cpa_guid) == 0 and len(cpa_guid) != 24:
                    message = 'Request id is not valid'
                    super_logger.info(f's3_file_name:')
                    super_logger.info(f'execution_status:FAIL')
                    super_logger.info(f'close_status_state:')
                    super_logger.info(f'close_status_code:')
                    super_logger.info(f'execution_date:{reporting_date}')
                    super_logger.info(f'ccpa_group:{group}')
                    super_logger.info(f'submission_date:')
                    super_logger.info(f'error_description:{message}')
                    logger.error(f'cpa_guid is blank or not valid:{err}')
                    logger.info(
                        f'**************************RequestID:{cpa_guid} is not valid for task:{task}******************************'
                    )

            except Exception as err:
                logger.error(f'Error occured:{err}')
        except Exception as err:
            logger.error(f'Other error occurred:{err}')


if __name__ == '__main__':
    ccpa_slingbox_delete()
    mysqlconn.close()
    logger.info(f'mysql Connection closed')

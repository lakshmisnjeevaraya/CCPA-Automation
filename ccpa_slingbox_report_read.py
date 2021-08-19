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
import time
from datetime import date
import aws_connection

moment = time.strftime("%Y-%b-%d__%H_%M_%S", time.localtime())
styleconfig = configparser.ConfigParser()
styleconfig.read('style.cfg')

config = configparser.ConfigParser()
config.read('resource.ini')

DBconfig = configparser.ConfigParser()
DBconfig.read('config.ini')

base_url = config.get('SNOW API-Dev2', 'base_url')
assignment_id = os.environ.get('assignment_id_sling')

query = config.get('Query-Fields', 'query').format(assignment_id)
Fields = config.get('Query-Fields', 'fields')
CustomTool = 'CPA-SlingDelete'
user = os.environ.get("sling_api_prod_user")
passw = os.environ.get("sling_api_prod_pwd")

mongoDB_uri = config.get('Mongo-API', 'mongourl')
tablename = config.get('mysql-table', 'sling_table')
role = config.get('S3-Role', 'role')
role_ccpa = config.get('S3-Role', 'ccparole')

selectquery = config.get('query', 'selectquery')

unloadquery = config.get('DB-Queries', 'unloadque')

# REdshift Credentials
dbname = DBconfig['Slingbox']['dbname']
dbuser = os.environ.get("slinguser")
host = DBconfig['Slingbox']['host']
dbpassword = os.environ.get("slingpassword")
port = DBconfig['Slingbox']['port']

#result for the process
Result = config.get('result', 'result_pass')

bq_project = config.get('bq', 'project_id')
dataset = config.get('bq', 'project_id')
bqtable = config.get('bq', 'project_id')

slingtable = config.get('Redshift-table', 'slinbox_table')

group = config.get('bq_table', 'slingbox_group')

# redshift connection
redshiftconn = redshift_connection.connection(dbname, dbuser, host, dbpassword,
                                              port)
# Hit SNOW API and get the Tasks and consumer info

# Decode the JSON response into a dictionary and use the data
# for each request ID that is still open, first get Mongo details (account/dishCustomerId)
# then connect to each database and insert records to Oracle to start our search processes.
# cpa_task = records['number']
# cpa_record_sys_id = records['sys_id']


def ccpa_slingbox_request():
    reporting_date = date.today()

    tasklist = http_client.get_snow_api(base_url, query, assignment_id, Fields,
                                        CustomTool, user, passw, group,
                                        reporting_date)
    totaltask = len(tasklist['result'])
    logger.info(f'Total number tasks: {totaltask}')

    for records in tasklist['result']:
        transaction_id = ''.join(random.choices(string.digits, k=5))
        super_logger.info(f'transction_id:{transaction_id}')
        super_logger.info(f'request_type:ReportTask')
        logger.info(f'Iterate Each task REcords values are :{records}')

        try:

            # strip off GUID/MongoID/RequestID from response.  It may be missing, so handle in try/catch
            task = records['number']
            super_logger.info(f'task_id:{task}')
            cpa_guid = records['parent.guid']
            super_logger.info(f'request_id:{cpa_guid}')
            sys_id = records['sys_id']
            keyobject = cpa_guid + "-" + task + "-" + "FSB.csv000"

            # keyobject = cpa_guid + '_' + task
            try:

                if len(cpa_guid) == 24:
                    # call Mongo API enpoint and capture response. This is an array
                    mongoresult = http_client.get_mongo_api(
                        mongoDB_uri, cpa_guid, CustomTool, task,
                        os.environ.get("mongo_user"), group, reporting_date)

                    for records2 in mongoresult['consumerPiiInfoList']:
                        if "email" in records2['consumer']:
                            emailId = records2['consumer']['email'][0]
                            try:
                                subdate = records2['request']['submissionDate']
                                submissiondate = str(
                                    dateutil.parser.parse(subdate).date())
                                reportlocation = records2['request'][
                                    'reportLocation']


                                print('**********************************')

                                print("reportlocationreportlocationreportlocation",reportlocation)

                                logger.info(
                                    f'submissiondate :{submissiondate}')
                                if emailId != "":
                                    selquery = config.get('query', 'selectquery')
                                    #
                                    # query_to_chk_data = build_query.selectslingquery(
                                    #     selectquery, slingtable, emailId,
                                    #     redshiftconn, submissiondate, group,
                                    #     reporting_date)
                                    #
                                    # logger.info(
                                    #     f'data presenet :{query_to_chk_data}')
                                    #
                                    # logger.info(f'task_id:{task}')


                                    print("task:", task)
                                    selectquery = selquery + ' ' + tablename + ' ' + "WHERE email_address = " + "'" + emailId + "'" + ';'
                                    print("selectquery:", selectquery)
                                    print("********************************",task)
                                    # deletequery = "DELETE FROM" + ' ' + tablename + ' ' + "WHERE email_address = " + "'" + emailId + "'" + ';'
                                    # print("deletequery:", deletequery)
                                    #
                                    # print('**********************************************************************')
                                else:
                                    print('**************')
                            except Exception as err:

                                logger.error(f'Error occured:{err}')
                        else:
                            print('**************')

                else:
                    print('**************')
            except Exception as err:

                print('**************')


        except Exception as err:

            logger.error(f'Error occured:{err}')





if __name__ == '__main__':
    ccpa_slingbox_request()
    logger.info(f'redshift Connection closed')

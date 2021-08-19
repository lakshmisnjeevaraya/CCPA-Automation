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
import aws_connection
import os
import random
import string
from setup import logger
from setup import super_logger
from datetime import date
from aws_connection import athena_connection_without_role
from aws_connection import athena_connection


styleconfig = configparser.ConfigParser()
styleconfig.read('style.cfg')

config = configparser.ConfigParser()
config.read('resource.ini')

DBconfig = configparser.ConfigParser()
DBconfig.read('config.ini')

base_url = config.get('SNOW API-Dev2', 'base_url')
assignment_id = config.get('SNOW API-Dev2', 'assignment_id_con')

query = config.get('Query-Fields', 'query').format(assignment_id)
Fields = config.get('Query-Fields', 'fields')
CustomTool = 'CPA-Conviva'
user = os.environ.get("con_api_prod_user")
passw = os.environ.get("com_api_prod_pwd")

mongoDB_uri = config.get('Mongo-API', 'mongourl')
tablename = config.get('Redshift-table', 'conviva_table')
tablename_prod = config.get('Redshift-table', 'conviva_table_prod')
tablename_dev = config.get('Redshift-table', 'conviva_dev_table')


role = config.get('S3-Role', 'role')
role_ccpa = config.get('S3-Role', 'ccparole')
role_arn = config.get('S3-Role', 'danyrole')
role_arn_new = config.get('S3-Role', 'newdanyrole')


unloadquery = config.get('DB-Queries', 'unloadque')

# REdshift Credentials
dbname = DBconfig['redshiftDB']['dbname']
dbuser = os.environ.get("user")
host = DBconfig['redshiftDB']['host']
dbpassword = os.environ.get("password")
port = DBconfig['redshiftDB']['port']

#result for the process
Result = config.get('result', 'result_pass')

bq_project = config.get('bq', 'project_id')
dataset = config.get('bq', 'project_id')
bqtable = config.get('bq', 'project_id')
request_type = 'ReportTask'

s3_staging_dir = "s3://ccpa-request"
s3_staging_dir_new = "s3://aws-athena-query-results-171459160518-us-west-2/ccpa-file"

s3_new="s3://p-datalake-resources/athena-results/"

s3_dev="s3://aws-athena-query-results-us-west-2-695893684697"


region_name = 'us-west-2'

# redshift connection
redshiftconn = redshift_connection.connection(dbname, dbuser, host, dbpassword,
                                              port)



athenaconn = athena_connection_without_role(
    os.environ.get("con_aws_access_key_id"),
    os.environ.get("con_aws_secret_access_key"), s3_staging_dir_new, region_name)

# athenaconn_new = athena_connection(
#     os.environ.get("con_aws_access_key_id"),
#     os.environ.get("con_aws_secret_access_key"), s3_staging_dir_new, region_name,role_arn_new)
#

# Hit SNOW API and get the Tasks and consumer info

# Decode the JSON response into a dictionary and use the data
# for each request ID that is still open, first get Mongo details (account/dishCustomerId)
# then connect to each database and insert records to Oracle to start our search processes.
# cpa_task = records['number']
# cpa_record_sys_id = records['sys_id']
group = config.get('bq_table', 'conviva_group')


def ccpa_conviva_request():
    reporting_date = date.today()
    # transaction_id = ''.join(random.choices(string.digits, k=5))

    tasklist = http_client.get_snow_api(base_url, query, assignment_id, Fields,
                                        CustomTool, user, passw, group,
                                        reporting_date)
    totaltask = len(tasklist['result'])
    logger.info(f'Total number tasks: {totaltask}')

    for records in tasklist['result']:
        transaction_id = ''.join(random.choices(string.digits, k=5))

        super_logger.info(f'transction_id:{transaction_id}')
        super_logger.info(f'request_type:{request_type}')

        logger.info(f'Iterate Each task REcords values are :{records}')

        try:

            # strip off GUID/MongoID/RequestID from response.  It may be missing, so handle in try/catch
            task = records['number']
            super_logger.info(f'task_id:{task}')
            cpa_guid = records['parent.guid']
            super_logger.info(f'request_id:{cpa_guid}')
            sys_id = records['sys_id']
            keyobject = cpa_guid + "-" + task + "-" + "FCC.csv"

            try:

                if len(cpa_guid) == 24:
                    # call Mongo API enpoint and capture response. This is an array
                    mongoresult = http_client.get_mongo_api(
                        mongoDB_uri, cpa_guid, CustomTool, task,
                        os.environ.get("mongo_user"), group, reporting_date)

                    for records2 in mongoresult['consumerPiiInfoList']:
                        try:
                            subdate = records2['request']['submissionDate']
                            submissiondate = str(
                                dateutil.parser.parse(subdate).date())
                            reportlocation = records2['request'][
                                'reportLocation']

                            if "internalVerification" in records2['request']:

                                for account_info in records2['request'][
                                        'internalVerification']:

                                    # now for each account ID element loop, request.actualRequestorProfile will hold CSG or DISH ID.

                                    # ultimately we need account number and provider ID

                                    provider_id = account_info['provider']
                                    extRefKey = account_info['extRefKey']
                                    account_number = account_info['result']
                                    key = account_info['extRefKey']
                                    name = key[0]['name']
                                    if (name == 'SlingID'):

                                        ViwershipID = key[0]['value']

                                        logger.info(
                                            f'provider_id :{provider_id}')
                                        logger.info(f'SlingID :{ViwershipID}')
                                        logger.info(
                                            f'submissiondate :{submissiondate}'
                                        )
                                        logger.info(
                                            f'reportLocation :{reportlocation}'
                                        )
                                        logger.info(f'tasknumber :{task}')

                                        # Validate ViwershipID present or not

                                        if ViwershipID != " ":

                                            query_to_chk_data = build_query.conviva_athena_query(
                                                unloadquery, tablename_prod,
                                                ViwershipID, submissiondate,
                                                athenaconn, reporting_date,
                                                group)
                                            print(query_to_chk_data)
                                            data = query_to_chk_data.fetchall()

                                            jobid = query_to_chk_data.query_id
                                            print('job id is:',jobid)

                                            # Validate data present for ViwershipID
                                            if data != []:
                                                # Aws Connection
                                                awsconnection = aws_connection.s3_connection_assumerole_danyrolw(
                                                    os.environ.get(
                                                        "con_aws_access_key_id"),
                                                    os.environ.get(
                                                        "con_aws_secret_access_key"
                                                    ), role_ccpa,
                                                    config.get(
                                                        'S3-Bucket',
                                                        'sourcebucket_dev'),
                                                    config.get(
                                                        'S3-Bucket',
                                                        'destinationBucket'),
                                                    reportlocation, jobid,
                                                    submissiondate, keyobject,
                                                    group, reporting_date)

                                                print('$$$$$$$$$$$$$$$$$')

                                                # closetask = http_client.delete_task(
                                                #     config.get(
                                                #         'DB-Queries',
                                                #         'deleteuri'), sys_id,
                                                #     keyobject, user, passw,
                                                #     config.get(
                                                #         'State-Closurecode-Report',
                                                #         'state'),
                                                #     config.get(
                                                #         'State-Closurecode-Report',
                                                #         'Data Provided'),
                                                #     submissiondate, group,
                                                #     reporting_date)

                                            # When data for ViwershipID is not present
                                            else:

                                                super_logger.info(
                                                    f's3_file_name:')

                                                logger.info(
                                                    f'No Data present for this task ID:{query_to_chk_data}'
                                                )
                                                closetask = http_client.delete_task(
                                                    config.get(
                                                        'DB-Queries',
                                                        'deleteuri'), sys_id,
                                                    keyobject, user, passw,
                                                    config.get(
                                                        'State-Closurecode-Report',
                                                        'state'),
                                                    config.get(
                                                        'State-Closurecode-Report',
                                                        'No Data Found'),
                                                    submissiondate, group,
                                                    reporting_date)

                                        else:

                                            super_logger.info(f's3_file_name:')

                                            closetask = http_client.delete_task(
                                                config.get(
                                                    'DB-Queries', 'deleteuri'),
                                                sys_id, keyobject, user, passw,
                                                config.get(
                                                    'State-Closurecode-Report',
                                                    'state'),
                                                config.get(
                                                    'State-Closurecode-Report',
                                                    'No Data Found'),
                                                submissiondate, group,
                                                reporting_date)
                                    else:
                                        super_logger.info(f's3_file_name:')
                                        closetask = http_client.delete_task(
                                            config.get('DB-Queries',
                                                       'deleteuri'), sys_id,
                                            keyobject, user, passw,
                                            config.get(
                                                'State-Closurecode-Report',
                                                'state'),
                                            config.get(
                                                'State-Closurecode-Report',
                                                'No Data Found'),
                                            submissiondate, group,
                                            reporting_date)
                                        logger.error(f'No SlingID not present')
                            else:
                                print(
                                    "*************No internal Verificatipon id present***********************"
                                )
                                super_logger.info(f's3_file_name:')
                                closetask = http_client.delete_task(
                                    config.get('DB-Queries', 'deleteuri'),
                                    sys_id, keyobject, user, passw,
                                    config.get('State-Closurecode-Report',
                                               'state'),
                                    config.get('State-Closurecode-Report',
                                               'No Data Found'),
                                    submissiondate, group, reporting_date)
                                logger.error(
                                    f'No internalVerification for CONVIVA : {cpa_guid + task}'
                                )

                        except Exception as err:
                            super_logger.info(f's3_file_name:')
                            super_logger.info(f'execution_status:FAIL')
                            super_logger.info(f'close_status_state:')
                            super_logger.info(f'close_status_code:')
                            super_logger.info(
                                f'execution_date:{reporting_date}')
                            super_logger.info(f'ccpa_group:{group}')
                            super_logger.info(
                                f'submission_date:{submissiondate}')
                            super_logger.info(f'error_description:{err}')

                            logger.info(
                                f'Error occurred in consumer info from Mongo API to get accountID/DciD:{err}'
                            )

                elif len(cpa_guid) == 0 and len(cpa_guid) != 24:
                    super_logger.info(f's3_file_name:')
                    super_logger.info(f'execution_status:FAIL')
                    super_logger.info(f'close_status_state:')
                    super_logger.info(f'close_status_code:')
                    super_logger.info(f'execution_date:{reporting_date}')
                    super_logger.info(f'ccpa_group:{group}')
                    super_logger.info(f'submission_date:{submissiondate}')
                    super_logger.info(f'error_description:')

                    logger.error(f'cpa_guid is blank or not valid:{err}')
            except Exception as err:
                logger.error(f'Error occured:{err}')

        except Exception as err:
            logger.error(f'Other error occurred:{err}')


if __name__ == '__main__':
    ccpa_conviva_request()
    redshiftconn.close()
    logger.info(f'Redshift Connection closed')

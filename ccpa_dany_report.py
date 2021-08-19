#############################################################
#  Purpose: connect to Service Now (Snow) and pull open DA tasks for consumer protection
#           then get Mongo API details (account_id/dishCustomerId) for said request
#           then call each database and insert the records that start our CPA processes
#  Author: Lakshmi Sanjeevaraya
#############################################################

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
import time
from datetime import date
from aws_connection import athena_connection

moment = time.strftime("%Y-%b-%d__%H_%M_%S", time.localtime())

filename2 = 'Logfile' + moment + '.log'

styleconfig = configparser.ConfigParser()
styleconfig.read('style.cfg')

config = configparser.ConfigParser()
config.read('resource.ini')

DBconfig = configparser.ConfigParser()
DBconfig.read('config.ini')

base_url = config.get('SNOW API-Dev2', 'base_url')
assignment_id = os.environ.get("assignment_id_dany")

query = config.get('Query-Fields', 'query').format(assignment_id)
Fields = config.get('Query-Fields', 'fields')
CustomTool = 'CPA-Dany'
user = os.environ.get("dan_api_prod_user")
passw = os.environ.get("dan_api_prod_pwd")
region_name = 'us-west-2'

mongoDB_uri = config.get('Mongo-API', 'mongourl')
tablename = config.get('Redshift-table', 'dany_table')
role = config.get('S3-Role', 'role')
role_arn = config.get('S3-Role', 'danyrole')
n_role_arn = config.get('S3-Role', 'newdanyrole')
role_ccpa = config.get('S3-Role', 'ccparole')

with open("dany_query") as file_in:
    for line in file_in:
        danyquery = line.strip()

dany_query1 = config.get('DB-Queries', 'select_dany')

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

s3_staging_dir = "s3://ccpa-request"
n_s3_staging_dir = "s3://p-output-conviva/daily/dedup"

region_name = 'us-west-2'
# redshift connection
# Hit SNOW API and get the Tasks and consumer info

athenaconn = athena_connection(os.environ.get("aws_access_key_id"),
                               os.environ.get("aws_secret_access_key"),
                               s3_staging_dir, region_name, role_arn)

# Decode the JSON response into a dictionary and use the data
# for each request ID that is still open, first get Mongo details (account/dishCustomerId)
# then connect to each database and insert records to Oracle to start our search processes.
# cpa_task = records['number']
# cpa_record_sys_id = records['sys_id']

group = config.get('bq_table', 'dany_group')


def ccpa_dany_request():
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
            keyobject = cpa_guid + "-" + task + "-" + "FCD.csv"

            try:

                if len(cpa_guid) == 24:

                    # call Mongo API enpoint and capture response. This is an array
                    use = 'lakshmi.sanjeevaraya'
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
                            # now for each account ID element loop, request.actualRequestorProfile will hold CSG or DISH ID.

                            if "actualRequestorProfile" in records2['request']:

                                for account_info in records2['request'][
                                        'actualRequestorProfile']:

                                    # ultimately we need account number and provider ID

                                    provider_id = account_info['providerId']
                                    print(
                                        "provider_idprovider_idprovider_idprovider_id",
                                        provider_id)
                                    # ShareInfoLength = provider_id.shareInfo.length
                                    # print("ShareInfoLengthShareInfoLengthShareInfoLengthShareInfoLength",ShareInfoLength)
                                    #

                                    if (provider_id == 'CSG-ACP'):

                                        account_id = account_info['extRefNum']
                                        print(
                                            "account_idaccount_idaccount_idaccount_idaccount_id",
                                            account_id)

                                        logger.info(
                                            f'provider_id :{provider_id}')
                                        logger.info(f'extRefNum :{account_id}')
                                        logger.info(
                                            f'submissiondate :{submissiondate}'
                                        )
                                        logger.info(
                                            f'reportLocation :{reportlocation}'
                                        )
                                        logger.info(f'tasknumber :{task}')

                                        # Validate ViwershipID present or not

                                        if account_id != " ":
                                            query_to_chk_data = build_query.dany_athena_query(
                                                account_id, submissiondate,
                                                athenaconn, group,
                                                reporting_date)

                                            dany_data = query_to_chk_data.fetchall(
                                            )
                                            jobid = query_to_chk_data.query_id

                                            # Validate data present for ViwershipID
                                            if dany_data != []:
                                                # Aws Connection
                                                awsconnection = aws_connection.s3_connection_assumerole_danyrolw(
                                                    os.environ.get(
                                                        "aws_access_key_id"),
                                                    os.environ.get(
                                                        "aws_secret_access_key"
                                                    ), role_ccpa,
                                                    config.get(
                                                        'S3-Bucket',
                                                        'sourceBucket'),
                                                    config.get(
                                                        'S3-Bucket',
                                                        'destinationBucket'),
                                                    reportlocation, jobid,
                                                    submissiondate, keyobject,
                                                    group, reporting_date)
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
                                                        'Data Provided'),
                                                    submissiondate, group,
                                                    reporting_date)
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

                                            # When data for ViwershipID is not present
                                        else:
                                            super_logger.info(f's3_file_name:')

                                            logger.info(
                                                f'No Data present for this task ID:{query_to_chk_data}'
                                            )
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
                                        # super_logger.info(f's3_file_name:')
                                        logger.error(
                                            f'provider id for CSG-ACP  is not present for DANY : {cpa_guid + task}'
                                        )

                                        closetask = http_client.delete_task(
                                            config.get('DB-Queries', 'deleteuri'),
                                            sys_id, keyobject, user, passw,
                                            config.get('State-Closurecode-Report',
                                                       'state'),
                                            config.get('State-Closurecode-Report',
                                                       'No Data Found'),
                                            submissiondate, group, reporting_date)

                            else:
                                super_logger.info(f's3_file_name:')
                                print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
                                closetask = http_client.delete_task(
                                    config.get('DB-Queries', 'deleteuri'),
                                    sys_id, keyobject, user, passw,
                                    config.get('State-Closurecode-Report',
                                               'state'),
                                    config.get('State-Closurecode-Report',
                                               'No Data Found'),
                                    submissiondate, group, reporting_date)

                                print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

                                logger.error(
                                    f'actualRequestorProfile section not there for DANY : {cpa_guid + task}'
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
                            super_logger.info(f'error_description:')

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
    ccpa_dany_request()
    logger.info(f'Redshift Connection closed')

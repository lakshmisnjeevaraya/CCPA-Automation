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
import Delete_BQ
import os
import random
import string
from setup import logger
from setup import super_logger
import time
from datetime import date
from aws_connection import athena_connection
import db_connection

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

query = config.get('Query-Fields', 'query_delete').format(assignment_id)
Fields = config.get('Query-Fields', 'fields')
CustomTool = 'CPA-Dany'
user = os.environ.get("dan_api_prod_user")
passw = os.environ.get("dan_api_prod_pwd")
region_name = 'us-west-2'

mongoDB_uri = config.get('Mongo-API', 'mongourl')
tablename = config.get('Redshift-table', 'dany_table')
table_athena = config.get('athena_table', 'account_table')
role = config.get('S3-Role', 'role')
role_arn = config.get('S3-Role', 'danyrole')
role_ccpa = config.get('S3-Role', 'ccparole')

with open("dany_query") as file_in:
    for line in file_in:
        danyquery = line.strip()

dany_query = config.get('DB-Queries', 'dany')

# REdshift Credentials
dbname = DBconfig['Slingbox']['dbname']
dbuser = os.environ.get("slinguse")
host = DBconfig['Slingbox']['host']
dbpassword = os.environ.get("slingpaw")
print("dbpassworddbpassworddbpassworddbpassword", dbpassword)
port = DBconfig['Slingbox']['port']

#result for the process
Result = config.get('result', 'result_pass')

bq_project = config.get('bq', 'project_id')
dataset = config.get('bq', 'project_id')
bqtable = config.get('bq', 'project_id')

s3_staging_dir = "s3://ccpa-request"
region_name = 'us-west-2'
# redshift connection
# Hit SNOW API and get the Tasks and consumer info

credentials = "/opt/dbadmin/ccpa/sling-studio-transfer-d377cc3410a8.json"
projectid = "sling-studio-transfer"

#Dany delete
credentials = '/Volumes/Data/sling-stream-recover-eb60c169ef8a.json'
projectid = 'sling-stream-recover'

bqconn = db_connection.big_query(credentials, projectid)

# bqconn = big_query(credentials, projectid)

# redshift connection
redshiftconn = redshift_connection.connection(dbname, dbuser, host, dbpassword,
                                              port)

athenaconn = athena_connection(os.environ.get("aws_access_key_id"),
                               os.environ.get("aws_secret_access_key"),
                               s3_staging_dir, region_name, role_arn)

# Decode the JSON response into a dictionary and use the data
# for each request ID that is still open, first get Mongo details (account/dishCustomerId)
# then connect to each database and insert records to Oracle to start our search processes.
# cpa_task = records['number']
# cpa_record_sys_id = records['sys_id']

group = config.get('bq_table', 'dany_group')


def ccpa_dany_delete_request():
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
                    mongoresult = http_client.get_mongo_api(
                        mongoDB_uri, cpa_guid, CustomTool, task,
                        os.environ.get("mongo_user"), group, reporting_date)

                    for records2 in mongoresult['consumerPiiInfoList']:
                        try:
                            subdate = records2['request']['submissionDate']
                            submissiondate = str(
                                dateutil.parser.parse(subdate).date())

                            if "actualRequestorProfile" in records2['request']:

                                # now for each account ID element loop, request.actualRequestorProfile will hold CSG or DISH ID.
                                for account_info in records2['request'][
                                        'actualRequestorProfile']:
                                    # ultimately we need account number and provider ID

                                    provider_id = account_info['providerId']
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

                                        logger.info(f'tasknumber :{task}')

                                        account = "'" + account_id + "'"

                                        selectvalue = []
                                        deletevalue = []
                                        afterdelselectvalue = []

                                        # Validate ViwershipID present or not

                                        if account_id != " ":
                                            print('*****************************')
                                            query_to_chk_data = build_query.select_query_tab(
                                                dany_query, table_athena,
                                                account, submissiondate,
                                                athenaconn, group,
                                                reporting_date)
                                            get_uuid = query_to_chk_data[0]

                                            print(
                                                "kjsdkahskdjhaskhdakjshdkjahsdkj",
                                                get_uuid)

                                            # Validate data present for ViwershipID
                                            if get_uuid != []:

                                                with open("tables.txt"
                                                          ) as file_in:
                                                    lines = []
                                                    for line in file_in:
                                                        lines.append(
                                                            line.strip())

                                                    # print("lineslineslineslines",lines)
                                                    length = len(lines)

                                                    # Iterating the index
                                                    # same as 'for i in range(len(list))'
                                                    for i in lines:
                                                        selectquery = "SELECT count(*) FROM" + " " + i + " " + "WHERE" + ' ' + 'lower' + '(' + 'uuid' + ')' + ' ' + 'like' + ' ' + 'lower' + '(' + "'" + get_uuid + "'" + ')' + ' ' + 'and' + ' ' + 'reporting_date' + ' ' + '<' + ' ' + 'DATE_TRUNC' + '(' + "'" + 'Day' + "'" + "," + 'sysdate' + ')' + ';' ""
                                                        print(selectquery)

                                                        select_data = build_query.query_redshift(
                                                            selectquery, i,
                                                            get_uuid,
                                                            submissiondate,
                                                            redshiftconn,
                                                            group,
                                                            reporting_date)

                                                        # selectvalue.append(select_data)
                                                        selectvalue.append(
                                                            str(select_data[0])
                                                        )

                                                        # Word = str(selectvalue[0])
                                                        # print("WordWordWordWordWord",Word)
                                                    print(
                                                        "selectvalueselectvalueselectvalueselectvalue",
                                                        selectvalue)

                                                    if any(value != '0'
                                                           for value in
                                                           selectvalue):

                                                        print(
                                                            "some of the values in select statement contains values"
                                                        )
                                                        for j in lines:
                                                            print(j)

                                                            deletequery = "DELETE FROM" + " " + j + " " + "WHERE" + " " + 'uuid' + ' ' + "=" + ' ' + "'" + get_uuid + "'"
                                                            print(
                                                                "deletequerydeletequerydeletequerydeletequery",
                                                                deletequery)
                                                            delete_data = build_query.delete_redshift(
                                                                deletequery, j,
                                                                get_uuid,
                                                                submissiondate,
                                                                redshiftconn,
                                                                group,
                                                                reporting_date)

                                                            deletevalue.append(
                                                                str(delete_data
                                                                    ))

                                                        print(
                                                            "deletevalue",
                                                            deletevalue)

                                                        dataDeletedinBQ = Delete_BQ.delete_data(
                                                            get_uuid)
                                                        print(
                                                            "Status od data deleted in BQ:",
                                                            dataDeletedinBQ)

                                                        for k in lines:
                                                            print(
                                                                "kkkkkkkkk", k)
                                                            selectquery = "SELECT count(*) FROM" + " " + k + " " + "WHERE" + ' ' + 'lower' + '(' + 'uuid' + ')' + ' ' + 'like' + ' ' + 'lower' + '(' + "'" + get_uuid + "'" + ')' + ' ' + 'and' + ' ' + 'reporting_date' + ' ' + '<' + ' ' + 'DATE_TRUNC' + '(' + "'" + 'Day' + "'" + "," + 'sysdate' + ')' + ';' ""
                                                            print(selectquery)

                                                            afterdelvalue = build_query.query_redshift(
                                                                selectquery, k,
                                                                get_uuid,
                                                                submissiondate,
                                                                redshiftconn,
                                                                group,
                                                                reporting_date)

                                                            afterdelselectvalue.append(
                                                                str(afterdelvalue[
                                                                    0]))
                                                        print(
                                                            "afterdelselectvalueafterdelselectvalueafterdelselectvalue",
                                                            afterdelselectvalue
                                                        )
                                                        print(
                                                            "*************************"
                                                        )

                                                        print(
                                                            "*********afterdelselectvalue************",
                                                            str(afterdelselectvalue
                                                                ))
                                                        print(
                                                            "*********deletevalue************",
                                                            str(deletevalue))

                                                        print(
                                                            "*************************"
                                                        )

                                                        if str(
                                                                selectvalue
                                                        ) == str(deletevalue):
                                                            if all(value == '0'
                                                                   for value in
                                                                   afterdelselectvalue
                                                                   ):
                                                                print(
                                                                    "################################"
                                                                )

                                                                closetask = http_client.delete_task(
                                                                    config.get(
                                                                        'DB-Queries',
                                                                        'deleteuri'
                                                                    ), sys_id,
                                                                    keyobject,
                                                                    user,
                                                                    passw,
                                                                    config.get(
                                                                        'State-Closurecode-Delete',
                                                                        'state'
                                                                    ),
                                                                    config.get(
                                                                        'State-Closurecode-Delete',
                                                                        'Data Deleted'
                                                                    ),
                                                                    submissiondate,
                                                                    group,
                                                                    reporting_date
                                                                )

                                                                print(
                                                                    'Data deleted for the taskID'
                                                                )

                                                        else:
                                                            message = "Select data and Delete data are not same"
                                                            super_logger.info(
                                                                f's3_file_name:'
                                                            )
                                                            super_logger.info(
                                                                f'execution_status:FAIL'
                                                            )
                                                            super_logger.info(
                                                                f'close_status_state:'
                                                            )
                                                            super_logger.info(
                                                                f'close_status_code:'
                                                            )
                                                            super_logger.info(
                                                                f'execution_date:{reporting_date}'
                                                            )
                                                            super_logger.info(
                                                                f'ccpa_group:{group}'
                                                            )
                                                            super_logger.info(
                                                                f'submission_date:{submissiondate}'
                                                            )
                                                            super_logger.info(
                                                                f'error_description:{message + "Select_value:" + afterdelselectvalue + "Delete_value:" + delete_data}'
                                                            )
                                                    else:
                                                        print(
                                                            "All values from select statement is 0"
                                                        )
                                                        super_logger.info(
                                                            f's3_file_name:')
                                                        closetask = http_client.delete_task(
                                                            config.get(
                                                                'DB-Queries',
                                                                'deleteuri'),
                                                            sys_id, keyobject,
                                                            user, passw,
                                                            config.get(
                                                                'State-Closurecode-Delete',
                                                                'state'),
                                                            config.get(
                                                                'State-Closurecode-Delete',
                                                                'No Data Found'
                                                            ), submissiondate,
                                                            group,
                                                            reporting_date)
                                                        print(
                                                            'No Data found for the taskID'
                                                        )

                                            else:
                                                super_logger.info(
                                                    f's3_file_name:')

                                                logger.info(
                                                    f'UUID is not present for this task ID:{query_to_chk_data}'
                                                )

                                                closetask = http_client.delete_task(
                                                    config.get(
                                                        'DB-Queries',
                                                        'deleteuri'), sys_id,
                                                    keyobject, user, passw,
                                                    config.get(
                                                        'State-Closurecode-Delete',
                                                        'state'),
                                                    config.get(
                                                        'State-Closurecode-Delete',
                                                        'No Data Found'),
                                                    submissiondate, group,
                                                    reporting_date)

                                            # When data for ViwershipID is not present
                                        else:
                                            super_logger.info(f's3_file_name:')

                                            logger.info(
                                                f'Account ID not present for this task ID:{query_to_chk_data}'
                                            )
                                            closetask = http_client.delete_task(
                                                config.get(
                                                    'DB-Queries', 'deleteuri'),
                                                sys_id, keyobject, user, passw,
                                                config.get(
                                                    'State-Closurecode-Delete',
                                                    'state'),
                                                config.get(
                                                    'State-Closurecode-Delete',
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
                                                'State-Closurecode-Delete',
                                                'state'),
                                            config.get(
                                                'State-Closurecode-Delete',
                                                'No Data Found'),
                                            submissiondate, group,
                                            reporting_date)
                                        logger.error(
                                            f'CSG-ACP  is not present for DANY : {cpa_guid + task}'
                                        )

                            else:
                                super_logger.info(f's3_file_name:')
                                closetask = http_client.delete_task(
                                    config.get('DB-Queries', 'deleteuri'),
                                    sys_id, keyobject, user, passw,
                                    config.get('State-Closurecode-Delete',
                                               'state'),
                                    config.get('State-Closurecode-Delete',
                                               'No Data Found'),
                                    submissiondate, group, reporting_date)
                                logger.error(
                                    f'actualRequestorProfile section for DANY : {cpa_guid + task}'
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

                else:
                    message = "Guid id is not valid"
                    super_logger.info(f's3_file_name:')
                    super_logger.info(f'execution_status:FAIL')
                    super_logger.info(f'close_status_state:')
                    super_logger.info(f'close_status_code:')
                    super_logger.info(f'execution_date:{reporting_date}')
                    super_logger.info(f'ccpa_group:{group}')
                    super_logger.info(f'submission_date:{submissiondate}')
                    super_logger.info(
                        f'error_description:{message} + {cpa_guid}')

                    logger.error(f'cpa_guid is blank or not valid:{err}')
            except Exception as err:
                logger.error(f'Error occured:{err}')

        except Exception as err:
            logger.error(f'Other error occurred:{err}')


if __name__ == '__main__':
    ccpa_dany_delete_request()
    logger.info(f'Redshift Connection closed')

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
import time
from datetime import date
from aws_connection import athena_connection_without_role


moment = time.strftime("%Y-%b-%d__%H_%M_%S", time.localtime())


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
role = config.get('S3-Role', 'role')
role_ccpa = config.get('S3-Role', 'ccparole')
role_arn = config.get('S3-Role', 'danyrole')


unloadquery = config.get('DB-Queries', 'unloadque')

# REdshift Credentials
dbname = DBconfig['redshiftDB']['dbname']
dbuser = os.environ.get("user")
host = DBconfig['redshiftDB']['host']
dbpassword = os.environ.get("password")
port = DBconfig['redshiftDB']['port']

#result for the process
Result=config.get('result', 'result_pass')

bq_project=config.get('bq','project_id')
dataset=config.get('bq','project_id')
bqtable=config.get('bq','project_id')
request_type='ReportTask'

s3_staging_dir="s3://ccpa-request"
region_name='us-west-2'

# redshift connection
redshiftconn = redshift_connection.connection(dbname, dbuser, host, dbpassword,
                                              port)

athenaconn=athena_connection_without_role(os.environ.get("aws_access_key_id"),os.environ.get("aws_secret_access_key"),s3_staging_dir, region_name)

# Hit SNOW API and get the Tasks and consumer info

# Decode the JSON response into a dictionary and use the data
# for each request ID that is still open, first get Mongo details (account/dishCustomerId)
# then connect to each database and insert records to Oracle to start our search processes.
# cpa_task = records['number']
# cpa_record_sys_id = records['sys_id']
group=config.get('bq_table','conviva_group')


def ccpa_conviva_request():
    reportlocation='cpa-5ea61f9188a47400850088d3'
    cpa='5ea61f9188a47400850088d3'
    submissiondate='2020-04-26'
    task='CPATSK0005806'
    ViwershipID='489bc308-20c4-11e7-9391-0e17176a94a9'
    reporting_date='2020-07-24'
    keyobject = cpa + "-" + task + "-" + "FCC.csv"

    query_to_chk_data = build_query.conviva_athena_query(unloadquery, tablename, ViwershipID, submissiondate,
                                                         athenaconn, reporting_date, group)

    jobid = query_to_chk_data.query_id


    awsconnection = aws_connection.s3_connection_assumerole_danyrolw(
        os.environ.get(
            "aws_access_key_id"),
        os.environ.get(
            "aws_secret_access_key"),
        role_ccpa,
        config.get(
            'S3-Bucket',
            'sourceBucket'),
        config.get(
            'S3-Bucket',
            'destinationBucket'),
        reportlocation, jobid, submissiondate, keyobject, group,
        reporting_date
    )


if __name__ == '__main__':
    ccpa_conviva_request()
    redshiftconn.close()
    logger.info(f'Redshift Connection closed')

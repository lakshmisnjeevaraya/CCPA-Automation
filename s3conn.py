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
import boto3



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
# tablename_new = config.get('Redshift-table', 'conviva_table_new')
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

s3_dev="s3://ccpa-file"

region_name = 'us-west-2'
reporting_date = date.today()

ViwershipID='11697be8-154e-11e9-abdf-0a2531cc9264'
subdate='2020-12-05 00:00:00'

submissiondate = str(
    dateutil.parser.parse(subdate).date())
group = config.get('bq_table', 'conviva_group')


guid='cpa-5fcc207b99fc8d0049ecd88d'
keyobject='5fcc207b99fc8d0049ecd88d-CPATSK0018613-FCC.csv'
destinationBucket='dish-cpa-report-staging-p'



# athenaconn = athena_connection_without_role(
#     os.environ.get("dev_aws_access_key_id"),
#     os.environ.get("dev_aws_secret_access_key"), s3_dev, region_name)


# query_to_chk_data = build_query.conviva_athena_query(
#                                                 unloadquery, tablename_dev,
#                                                 ViwershipID, submissiondate,
#                                                 athenaconn, reporting_date,
#                                                 group)
# print(query_to_chk_data)
# data = query_to_chk_data.fetchall()
#
# jobid = query_to_chk_data.query_id
# print('job id is:',jobid)

s3 = boto3.resource('s3',aws_access_key_id=os.environ.get("dev_aws_access_key_id"),aws_secret_access_key=os.environ.get("dev_aws_secret_access_key"))
for bucket in s3.buckets.all():
    print (bucket.name)

# copy_source = {
# 'Bucket': 'aws-athena-query-results-171459160518-us-west-2/ccpa-file/',
# 'Key': jobid + '.csv'
# }


# awsconnection = aws_connection.s3_connection_assumerole_conviva(
#                                                     os.environ.get(
#                                                         "con_aws_access_key_id"),
#                                                     os.environ.get(
#                                                         "con_aws_secret_access_key"
#                                                     ), role_arn_new,
#                                                     config.get(
#                                                         'S3-Bucket',
#                                                         'sourceBucket_new'),
#                                                     config.get(
#                                                         'S3-Bucket',
#                                                         'destinationBucket'),
#                                                     reportlocation, jobid,
#                                                     submissiondate, keyobject,
#                                                     group, reporting_date)
#
#                                                 print('$$$$$$$$$$$$$$$$$')


SOURCE_BUK='ccpa-file'

# s3://aws-athena-query-results-171459160518-us-west-2/ccpa-file/

print("SOURCE_BUKSOURCE_BUKSOURCE_BUKSOURCE_BUK",SOURCE_BUK)


# copy_source = {'Bucket': SOURCE_BUK + '/ccpa_file/', 'Key': jobid + '.csv'}

copy_source = {'Bucket': SOURCE_BUK, 'Key': jobid + '.csv'}


# //aws-athena-query-results-171459160518-us-west-2/ccpa_file/
print("copy_sourcecopy_sourcecopy_sourcecopy_source",copy_source)

destinationplace = guid + '/' + keyobject

buckett = s3.Bucket(destinationBucket)
# buccopy = bucket.copy(copy_source, destinationplace)

buckett.copy(copy_source,destinationplace)

# copy_source = {'Bucket': sourceBucket, 'Key': key + '.csv'}
        # print("copy_sourcecopy_sourcecopy_sourcecopy_sourcecopy_source",
        #           copy_source)
        # destinationplace = guid + '/' + keyobject
        #
        # bucket = boto_sts.Bucket(destinationBucket)
        # print("***************************************************")
        # print("bucketbucketbucketbucketbucketbucket", bucket)
        # buccopy = bucket.copy(copy_source, destinationplace)


# bucket = s3.Bucket(destinationBucket)
# bucket.copy(copy_source, 'otherkey')
# destinationBucket=aws-athena-query-results-171459160518-us-west-2
# bucket = s3.Bucket(destinationBucket)
# bucket.copy(copy_source,  key + '.csv')

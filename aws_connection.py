#############################################################
#  Purpose: Contains all the AWS DB connections
#  Author: Lakshmi.Sanjeevaraya
#############################################################

import boto3
from pyathena import connect
from setup import super_logger
from setup import logger
from google.oauth2 import service_account
from google.cloud import bigquery
from boto3.session import Session


# Aws athena connection
def athena_connection(AWS_KEY_ID, AWS_SECRET, s3_dir, region_name,
                      arn):
    try:
        # declare a new Athena connection object

        cursor = connect(aws_access_key_id=AWS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET,
                         s3_staging_dir=s3_dir,
                         region_name=region_name,
                         role_arn=arn).cursor()

        logger.info(f'AWS Connection established Successfully')

        logger.info(f'AWS Connection established Successfully:{cursor}')
        return cursor

    except Exception as err:
        print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        logger.error(f'Athena connect() ERROR:{err}')
        cursor = None


# Aws athena connection
def athena_connection_without_role(AWS_KEY_ID, AWS_SECRET, s3_staging_dir,
                                   region_name):
    try:
        # declare a new Athena connection object

        cursor = connect(aws_access_key_id=AWS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET,
                         s3_staging_dir=s3_staging_dir,
                         region_name=region_name).cursor()

        logger.info(f'AWS Connection established Successfully')
        logger.info(f'AWS Connection established Successfully:{cursor}')
        return cursor

    except Exception as err:
        print('*********************')
        logger.error(f'Athena connect() ERROR:{err}')
        cursor = None


# Aws s3 connection
def s3_connection(aws_access_key_id, aws_secret_access_key):
    try:
        s3 = boto3.client('s3://ccpa-request/ccpa/test', aws_access_key_id,
                          aws_secret_access_key)
        logger.info(f'AWS S3 Connection established Successfully:')
        s3.download_file(aws_access_key_id, aws_secret_access_key)
        return s3
    except Exception as err:
        logger.error(f'AWS S3 Connection failed:{err}')
        s3 = None


# Assume_role connection
def s3_connection_assumerole(aws_access_key_id, aws_secret_access_key, role,
                             sourceBucket, destinationBucket, guid, key,
                             submissiondate, group, reporting_date):
    try:
        boto_sts = boto3.client('sts',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

        # Request to assume the role like this, the ARN is the Role's ARN from
        # the other account you wish to assume. Not your current ARN.
        stsresponse = boto_sts.assume_role(RoleArn=role,
                                           RoleSessionName='test')

        if stsresponse is not None:
            # Save the details from assumed role into vars

            newsession_id = stsresponse["Credentials"]["AccessKeyId"]
            newsession_key = stsresponse["Credentials"]["SecretAccessKey"]
            newsession_token = stsresponse["Credentials"]["SessionToken"]

            extra_args = {'ACL': 'bucket-owner-full-control'}

            # Here I create an s3 resource with the assumed creds
            s3_assumed_client = boto3.client(
                's3',
                region_name='us-west-2',
                aws_access_key_id=newsession_id,
                aws_secret_access_key=newsession_key,
                aws_session_token=newsession_token)
            # Here I create an s3 resource with the assumed creds
            s3_assumed_resource = boto3.resource(
                's3',
                region_name='us-west-2',
                aws_access_key_id=newsession_id,
                aws_secret_access_key=newsession_key,
                aws_session_token=newsession_token,
            )

            response = s3_assumed_client.list_buckets()

            copy_source = {'Bucket': sourceBucket, 'Key': key}
            destinationplace = guid + '/' + key.strip("000")

            bucket = s3_assumed_resource.Bucket(destinationBucket)
            print("***************************************************")
            buccopy = bucket.copy(copy_source, destinationplace, extra_args)
            if buccopy is None:
                logger.info(
                    f' Uploaded csv file to AWS S3(CCPA Bucket) successfully in :{destinationplace}'
                )
                super_logger.info(f's3_file_name:{destinationplace}')

            else:
                logger.info(
                    f' Failed to uploaded csv file to AWS S3(CCPA Bucket) in :{destinationplace}'
                )
                logger.info(
                    f' ************Failed to uploaded csv file to AWS S3(CCPA Bucket) in :{destinationplace}****************'
                )
                super_logger.info(f's3_file_name:')
                super_logger.info(f'execution_status:FAIL')
                super_logger.info(f'close_status_state:')
                super_logger.info(f'close_status_code:')
                super_logger.info(f'execution_date:{reporting_date}')
                super_logger.info(f'ccpa_group:{group}')
                super_logger.info(f'submission_date:{submissiondate}')
                super_logger.info(f'error_description:')
                exit()
        else:
            logger.error(f'Role switching failed :{stsresponse}')
            super_logger.info(f's3_file_name:')
            super_logger.info(f'execution_status:FAIL')
            super_logger.info(f'close_status_state:')
            super_logger.info(f'close_status_code:')
            super_logger.info(f'execution_date:{reporting_date}')
            super_logger.info(f'ccpa_group:{group}')
            super_logger.info(f'submission_date:{submissiondate}')
            super_logger.info(f'error_description:')

            exit()

    except Exception as err:
        logger.error(f'Connection to AWS s3 failed:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')

        exit()

        s3 = None


# Assume_role connection
def s3_connection_assumerole_danyrolw(aws_access_key_id, aws_secret_access_key,
                                      role, sourceBucket, destinationBucket,
                                      guid, key, submissiondate, keyobject,
                                      group, reporting_date):


    print("guidguidguidguidguidguidguidguidguidguid", guid)
    try:
        boto_sts = boto3.client('sts',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)



        # Request to assume the role like this, the ARN is the Role's ARN from
        # the other account you wish to assume. Not your current ARN.
        stsresponse = boto_sts.assume_role(RoleArn=role,
                                           RoleSessionName='test')

        if stsresponse is not None:
            # Save the details from assumed role into vars

            newsession_id = stsresponse["Credentials"]["AccessKeyId"]
            newsession_key = stsresponse["Credentials"]["SecretAccessKey"]
            newsession_token = stsresponse["Credentials"]["SessionToken"]

            extra_args = {'ACL': 'bucket-owner-full-control'}

            # Here I create an s3 resource with the assumed creds
            s3_assumed_client = boto3.client(
                's3',
                region_name='us-west-2',
                aws_access_key_id=newsession_id,
                aws_secret_access_key=newsession_key,
                aws_session_token=newsession_token)
            # Here I create an s3 resource with the assumed creds
            s3_assumed_resource = boto3.resource(
                's3',
                region_name='us-west-2',
                aws_access_key_id=newsession_id,
                aws_secret_access_key=newsession_key,
                aws_session_token=newsession_token,
            )

            response = s3_assumed_client.list_buckets()
            print("&&&&&&&&&&&&&&&&&&&&")

            copy_source = {'Bucket': sourceBucket, 'Key': key + '.csv'}
            print("copy_sourcecopy_sourcecopy_sourcecopy_sourcecopy_source",
                  copy_source)
            destinationplace = guid + '/' + keyobject

            bucket = s3_assumed_resource.Bucket(destinationBucket)
            print("***************************************************")
            print("bucketbucketbucketbucketbucketbucket",bucket)
            buccopy = bucket.copy(copy_source, destinationplace, extra_args)

            print("buccopybuccopybuccopybuccopybuccopy",buccopy)

            if buccopy is None:
                logger.info(
                    f' Uploaded csv file to AWS S3(CCPA Bucket) successfully in :{destinationplace}'
                )
                super_logger.info(f's3_file_name:{destinationplace}')

            else:
                logger.info(
                    f' Failed to uploaded csv file to AWS S3(CCPA Bucket) in :{destinationplace}'
                )
                logger.info(
                    f' ************Failed to uploaded csv file to AWS S3(CCPA Bucket) in :{destinationplace}****************'
                )
                super_logger.info(f's3_file_name:')
                super_logger.info(f'execution_status:FAIL')
                super_logger.info(f'close_status_state:')
                super_logger.info(f'close_status_code:')
                super_logger.info(f'execution_date:{reporting_date}')
                super_logger.info(f'ccpa_group:{group}')
                super_logger.info(f'submission_date:{submissiondate}')
                super_logger.info(f'error_description:')
                exit()
        else:
            logger.error(f'Role switching failed :{stsresponse}')
            super_logger.info(f's3_file_name:')
            super_logger.info(f'execution_status:FAIL')
            super_logger.info(f'close_status_state:')
            super_logger.info(f'close_status_code:')
            super_logger.info(f'execution_date:{reporting_date}')
            super_logger.info(f'ccpa_group:{group}')
            super_logger.info(f'submission_date:{submissiondate}')
            super_logger.info(f'error_description:')

            exit()

    except Exception as err:
        logger.error(f'Connection to AWS s3 failed:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')

        exit()

        s3 = None


# Assume_role connection
def aws_conn_dany_assumerole(aws_access_key_id, aws_secret_access_key, role,
                             sourceBucket, accountid):
    try:
        boto_sts = boto3.client('sts',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

        # Request to assume the role like this, the ARN is the Role's ARN from
        # the other account you wish to assume. Not your current ARN.
        stsresponse = boto_sts.assume_role(RoleArn=role,
                                           RoleSessionName='test')

        if stsresponse is not None:
            # Save the details from assumed role into vars

            newsession_id = stsresponse["Credentials"]["AccessKeyId"]
            newsession_key = stsresponse["Credentials"]["SecretAccessKey"]
            newsession_token = stsresponse["Credentials"]["SessionToken"]

            extra_args = {'ACL': 'bucket-owner-full-control'}

            # Here I create an s3 resource with the assumed creds
            s3_assumed_resource = boto3.resource(
                'athena',
                region_name='us-west-2',
                aws_access_key_id=newsession_id,
                aws_secret_access_key=newsession_key,
                aws_session_token=newsession_token,
                bucket='ccpa-request',
                path='ccpa-request/ccpa',
            )

            s3_assumed_resource.execute(
                "SELECT * FROM viewership_conviva.conviva_dedup limit 10")
            print("***************")

            print(s3_assumed_resource.description)
            print(s3_assumed_resource.fetchall())

        else:
            print('error')

    except Exception as err:
        logger.error(f'Connection to AWS s3 failed:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group: ')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')

        exit()

        s3 = None




def big_query(credentials, project_id):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials)
        client = bigquery.Client(credentials=credentials, project=project_id)
        query = "Select * from `sling-studio-transfer.studio.ccpa_automation_log` limit 10"

        job = client.query(query)
        result = job.result()
        for row in result:
            print("rowrowrowrowrowrowrow", row)

        return row
    except Exception as e:
        print("Test")


# Assume_role connection
def s3_connection_assumerole_conviva(aws_access_key_id, aws_secret_access_key,
                                      role, sourceBucket, destinationBucket,
                                      guid, key, submissiondate, keyobject,
                                      group, reporting_date):
    print("guidguidguidguidguidguidguidguidguidguid", guid)

    print("keyobjectkeyobjectkeyobject", keyobject)

    print("destinationBucketdestinationBucket", destinationBucket)



    try:

        s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)


        # s3 = boto3.resource('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
        print(s3.list_buckets)
        # copy_source = {
        #     'Bucket': 'aws-athena-query-results-171459160518-us-west-2/ccpa-file',
        #     'Key': key
        # }
        # bucket = s3.Bucket(destinationBucket)
        # bucket.copy(copy_source,  key + '.csv')



        # s3 = boto3.client('s3://aws-athena-query-results-171459160518-us-west-2/ccpa-file', aws_access_key_id,
        #                   aws_secret_access_key)
        # logger.info(f'AWS S3 Connection established Successfully:')
        # s3.download_file(aws_access_key_id, aws_secret_access_key)

        # print(bucket)



        # boto_sts = boto3.resource('s3',
        #                         aws_access_key_id=aws_access_key_id,
        #                         aws_secret_access_key=aws_secret_access_key)
        # src = boto_sts.Bucket(sourceBucket)
        # print("srcsrcsrcsrcsrcsrcsrcsrcsrcsrcsrcsrcsrcsrcsrc",src)
        # copy_source = {'Bucket': sourceBucket, 'Key': key + '.csv'}
        # print("copy_sourcecopy_sourcecopy_sourcecopy_sourcecopy_source",
        #           copy_source)
        # destinationplace = guid + '/' + keyobject
        #
        # bucket = boto_sts.Bucket(destinationBucket)
        # print("***************************************************")
        # print("bucketbucketbucketbucketbucketbucket", bucket)
        # buccopy = bucket.copy(copy_source, destinationplace)

        # print("buccopybuccopybuccopybuccopybuccopy", buccopy)
        # if buccopy is None:
        #     logger.info(
        #         f' Uploaded csv file to AWS S3(CCPA Bucket) successfully in :{destinationplace}'
        #     )
        #     super_logger.info(f's3_file_name:{destinationplace}')
        #
        # else:
        #     logger.info(
        #         f' Failed to uploaded csv file to AWS S3(CCPA Bucket) in :{destinationplace}'
        #     )
        #     logger.info(
        #         f' ************Failed to uploaded csv file to AWS S3(CCPA Bucket) in :{destinationplace}****************'
        #     )
        #     super_logger.info(f's3_file_name:')
        #     super_logger.info(f'execution_status:FAIL')
        #     super_logger.info(f'close_status_state:')
        #     super_logger.info(f'close_status_code:')
        #     super_logger.info(f'execution_date:{reporting_date}')
        #     super_logger.info(f'ccpa_group:{group}')
        #     super_logger.info(f'submission_date:{submissiondate}')
        #     super_logger.info(f'error_description:')
        #     exit()




        # # Request to assume the role like this, the ARN is the Role's ARN from
        # # the other account you wish to assume. Not your current ARN.
        # stsresponse = boto_sts.assume_role(RoleArn=role,
        #                                    RoleSessionName='test')
        #
        # if stsresponse is not None:
        #     # Save the details from assumed role into vars
        #
        #     newsession_id = stsresponse["Credentials"]["AccessKeyId"]
        #     newsession_key = stsresponse["Credentials"]["SecretAccessKey"]
        #     newsession_token = stsresponse["Credentials"]["SessionToken"]
        #
        #     extra_args = {'ACL': 'bucket-owner-full-control'}
        #
        #     # Here I create an s3 resource with the assumed creds
        #     s3_assumed_client = boto3.client(
        #         's3',
        #         region_name='us-west-2',
        #         aws_access_key_id=newsession_id,
        #         aws_secret_access_key=newsession_key,
        #         aws_session_token=newsession_token)
        #     # Here I create an s3 resource with the assumed creds
        #     s3_assumed_resource = boto3.resource(
        #         's3',
        #         region_name='us-west-2',
        #         aws_access_key_id=newsession_id,
        #         aws_secret_access_key=newsession_key,
        #         aws_session_token=newsession_token,
        #     )
        #
        #     response = s3_assumed_client.list_buckets()
        #     print("&&&&&&&&&&&&&&&&&&&&")
        #
        #     copy_source = {'Bucket': sourceBucket, 'Key': key + '.csv'}
        #     print("copy_sourcecopy_sourcecopy_sourcecopy_sourcecopy_source",
        #           copy_source)
        #     destinationplace = guid + '/' + keyobject
        #
        #     bucket = s3_assumed_resource.Bucket(destinationBucket)
        #     print("***************************************************")
        #     print("bucketbucketbucketbucketbucketbucket", bucket)
        #     buccopy = bucket.copy(copy_source, destinationplace, extra_args)
        #
        #     print("buccopybuccopybuccopybuccopybuccopy", buccopy)
        #
        #     if buccopy is None:
        #         logger.info(
        #             f' Uploaded csv file to AWS S3(CCPA Bucket) successfully in :{destinationplace}'
        #         )
        #         super_logger.info(f's3_file_name:{destinationplace}')
        #
        #     else:
        #         logger.info(
        #             f' Failed to uploaded csv file to AWS S3(CCPA Bucket) in :{destinationplace}'
        #         )
        #         logger.info(
        #             f' ************Failed to uploaded csv file to AWS S3(CCPA Bucket) in :{destinationplace}****************'
        #         )
        #         super_logger.info(f's3_file_name:')
        #         super_logger.info(f'execution_status:FAIL')
        #         super_logger.info(f'close_status_state:')
        #         super_logger.info(f'close_status_code:')
        #         super_logger.info(f'execution_date:{reporting_date}')
        #         super_logger.info(f'ccpa_group:{group}')
        #         super_logger.info(f'submission_date:{submissiondate}')
        #         super_logger.info(f'error_description:')
        #         exit()
        # else:
        #     logger.error(f'Role switching failed :{stsresponse}')
        #     super_logger.info(f's3_file_name:')
        #     super_logger.info(f'execution_status:FAIL')
        #     super_logger.info(f'close_status_state:')
        #     super_logger.info(f'close_status_code:')
        #     super_logger.info(f'execution_date:{reporting_date}')
        #     super_logger.info(f'ccpa_group:{group}')
        #     super_logger.info(f'submission_date:{submissiondate}')
        #     super_logger.info(f'error_description:')
        #
        #     exit()

    except Exception as err:
        logger.error(f'Connection to AWS s3 failed:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group:{group}')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')

        exit()

        s3 = None


def aws_conn(aws_access_key_id, aws_secret_access_key, role,
                             sourceBucket, accountid):
    try:
        boto_sts = boto3.client('sts',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

        # Request to assume the role like this, the ARN is the Role's ARN from
        # the other account you wish to assume. Not your current ARN.
        stsresponse = boto_sts.assume_role(RoleArn=role,
                                           RoleSessionName='test')

        if stsresponse is not None:
            # Save the details from assumed role into vars

            newsession_id = stsresponse["Credentials"]["AccessKeyId"]
            newsession_key = stsresponse["Credentials"]["SecretAccessKey"]
            newsession_token = stsresponse["Credentials"]["SessionToken"]

            extra_args = {'ACL': 'bucket-owner-full-control'}

            # Here I create an s3 resource with the assumed creds
            s3_assumed_resource = boto3.resource(
                'athena',
                region_name='us-west-2',
                aws_access_key_id=newsession_id,
                aws_secret_access_key=newsession_key,
                aws_session_token=newsession_token,
                bucket='ccpa-request',
                path='ccpa-request/ccpa',
            )

            s3_assumed_resource.execute(
                "SELECT * FROM viewership_conviva.conviva_dedup limit 10")
            print("***************")

            print(s3_assumed_resource.description)
            print(s3_assumed_resource.fetchall())

        else:
            print('error')

    except Exception as err:
        logger.error(f'Connection to AWS s3 failed:{err}')
        super_logger.info(f's3_file_name:')
        super_logger.info(f'close_status_state:')
        super_logger.info(f'close_status_code:')
        super_logger.info(f'execution_date:{reporting_date}')
        super_logger.info(f'ccpa_group: ')
        super_logger.info(f'submission_date:{submissiondate}')
        super_logger.info(f'error_description:{err}')

        exit()

        s3 = None




def s3_connection1(aws_access_key_id, aws_secret_access_key,sourceBucket,destinationBucket,guid,key,keyobject):
    # s3 = boto3.client('s3://ccpa-request/ccpa/test', aws_access_key_id,
    #                   aws_secret_access_key)
    # logger.info(f'AWS S3 Connection established Successfully:')
    # s3.download_file(aws_access_key_id, aws_secret_access_key)

    session = Session(aws_access_key_id=os.environ.get("con_aws_access_key_id"),
                      aws_secret_access_key=os.environ.get("con_aws_secret_access_key"))
    s3 = session.resource('s3')
    your_bucket = s3.Bucket('aws-athena-query-results-171459160518-us-west-2')




    # s3 = boto3.resource('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    #
    # # copy_source = {'Bucket': SOURCE_BUK + '/ccpa_file/', 'Key': jobid + '.csv'}

    copy_source = {'Bucket': sourceBucket, 'Key': key + '.csv'}


    # //aws-athena-query-results-171459160518-us-west-2/ccpa_file/
    print("copy_sourcecopy_sourcecopy_sourcecopy_source",copy_source)

    destinationplace = guid + '/' + keyobject
    print("*********************************************************************************")

    buckett = s3.Bucket(destinationBucket)
    # buccopy = bucket.copy(copy_source, destinationplace)

    print("-------------------------------------------------------------------------------",buckett)


    buckett.copy(copy_source,destinationplace)


    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")




if __name__ == '__main__':
    s3_connection('access_key', 'secret_key')
    athena_connection('access_key', 'secret_key', 's3_staging_dir',
                      'region_name','rn')
    s3_connection_assumerole('aws_access_key_id', 'aws_secret_access_key',
                             'role', 'sourceBucket', 'destinationBucket',
                             'guid', 'key')

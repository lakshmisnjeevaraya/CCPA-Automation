# import boto3
# s3 = boto3.resource('s3')
# bucket = s3.Bucket('p-input-social-media')
# s3.Object('mybucket', 'hello.txt').put(Body=open('/tmp/hello.txt', 'rb'))
#


import boto3

# aws_access_key_id='AKIAULH4FTLJFNU533VD'
# aws_secret_access_key='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'

aws_access_key_id='AKIASP26DFHDAZBXOLMT'
aws_secret_access_key='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'


s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

s3.upload_file('/Volumes/Data/DataAnalytics/Interview/dataset.xlsx', 'p-input-social-media', 'dataset.xlsx')
print("Upload Successful")


# # s3 = boto3.client('s3://p-input-social-media', aws_access_key_id,
# #                           aws_secret_access_key)
# # print('AWS S3 Connection established Successfully:')
# #         s3.download_file(aws_access_key_id, aws_secret_access_key)
# # cursor = connect(aws_access_key_id='AKIAULH4FTLJFNU533VD',
# #                          aws_secret_access_key='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',
# #                          s3_staging_dir='s3://aws-athena-query-results-171459160518-us-west-2/ccpa-file',
# #                          region_name='us-west-2').cursor()
# # s3 = boto3.resource('s3')
# s3.meta.client.upload_file('/Volumes/Data/DataAnalytics/Interview/dataset.xlsx', 'p-input-social-media', 'dataset.xlsx')
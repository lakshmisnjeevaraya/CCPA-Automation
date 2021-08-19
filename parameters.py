import configparser
import os

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

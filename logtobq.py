from collections import defaultdict
import pprint
import configparser
from google.cloud import bigquery
from google.oauth2 import service_account
import glob
import os
import pandas as pd
import time
import uploaddatatobq

DBconfig = configparser.ConfigParser()
DBconfig.read('resource.ini')

credentials = '/Volumes/Data/Tableau/CCPA/sling-studio-transfer-d377cc3410a8.json'

bq_project = DBconfig.get('bq', 'project_id')
dataset = DBconfig.get('bq', 'dataset_id')
bqtable = DBconfig.get('bq', 'table_id')
# bqtable='ccpaautomation'

credentials = service_account.Credentials.from_service_account_file(
    credentials)
client = bigquery.Client(credentials=credentials, project=bq_project)

content_array = []
my_dict = {}


def loadlogfiletobigquery():

    # content_array = []
    # my_dict = {}
    logfile = glob.iglob(
        '/Users/lakshmi.sanjeevaraya/PycharmProjects/ccpa_automation_new_dev/Logfile/BQfile'
        + '*.log')
    print(logfile)

    latest_log_file = max(logfile, key=os.path.getctime)
    print(latest_log_file)

    with open(latest_log_file) as f:
        # Content_list is the list that contains the read lines.
        for line in f:
            content_array.append(line.strip())

        print(content_array)

        my_dict = defaultdict(list)
        current_key = None

        for item in content_array:
            print(item)
            transac = item.split()
            print(transac)
            if ':' in item:
                current_key, value = item.split(':')

            my_dict[current_key].append(value)

        my_dict = {k: v[0] if len(v) == 0 else v for k, v in my_dict.items()}
        print("*********************", my_dict)
        # my_dict = {k: int(v[0]) if type(v) is str else None  for k, v in my_dict.items()}

        df = pd.DataFrame(my_dict,
                          columns=[
                              'transction_id', 'request_type', 'task_id',
                              'request_id', 's3_file_name', 'execution_status',
                              'close_status_state', 'close_status_code',
                              'execution_date', 'ccpa_group',
                              'submission_date', 'error_description'
                          ])

        df.to_csv(
            "/Users/lakshmi.sanjeevaraya/PycharmProjects/ccpa_automation_new_dev/Logfile/logtocsv"
            + time.strftime('%Y-%m-%d-%H-%S') + ".csv",
            ',',
            index=False,
            header=False)
        time.sleep(10)
        print(df)

        newcsv = glob.iglob(
            '/Users/lakshmi.sanjeevaraya/PycharmProjects/ccpa_automation_new_dev/Logfile/logtocsv'
            + '*.csv')

        latest_csv = max(newcsv, key=os.path.getctime)

        uploaddatatobq.uploadtobq(latest_csv, bq_project, dataset, bqtable)

    # newcsv = glob.iglob('/Volumes/Data/Tableau/CCPA/BQ_data_uploaded.csv')
    # #
    # latest_csv = max(newcsv, key=os.path.getctime)
    #
    # uploaddatatobq.uploadtobq(latest_csv, bq_project, dataset, bqtable)


if __name__ == '__main__':
    loadlogfiletobigquery()

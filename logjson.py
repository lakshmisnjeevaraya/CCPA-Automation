
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
import json


DBconfig = configparser.ConfigParser()
DBconfig.read('resource.ini')

credentials ='/Volumes/Data/Tableau/CCPA/sling-studio-transfer-d377cc3410a8.json'

bq_project=DBconfig.get('bq','project_id')
dataset=DBconfig.get('bq','dataset_id')
bqtable=DBconfig.get('bq','table_id')
# bqtable='ccpaautomation'



credentials = service_account.Credentials.from_service_account_file(credentials)
client = bigquery.Client(credentials=credentials, project=bq_project)

content_array = []
my_dict = {}

def loadlogfiletobigquery():


    # content_array = []
    # my_dict = {}
    logfile = glob.iglob('/Users/lakshmi.sanjeevaraya/PycharmProjects/ccpa_automation_new_dev/Logfile/BQfile' + '*.log')
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
            transac=item.split()
            print(transac)
            if ':' in item:
                current_key, value = item.split(':')

            my_dict[current_key].append(value)

        my_dict = {k: v[0] if len(v) == 0  else v for k, v in my_dict.items()}
        print("*********************",my_dict)

        json_object = json.dumps(my_dict, indent=12)
        print ("json", json_object)

        with open("sample.json", "w") as outfile:
            outfile.write(json_object)


if __name__ == '__main__':
    loadlogfiletobigquery()


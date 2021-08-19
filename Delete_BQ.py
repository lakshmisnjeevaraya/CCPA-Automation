import db_connection
import configparser

import build_query
from setup import super_logger

config = configparser.ConfigParser()
config.read('config.ini')

credentials = '/Volumes/Data/sling-stream-recover-eb60c169ef8a.json'
projectid = 'sling-stream-recover'

reportingdate = config.get('reporting_date', 'reportingdate')

# Schema
bq_project = config.get('bigquery', 'project_id')

bqschema = config['schema']['bqschema']
redshiftbq = config['schema']['redshiftschema']

# Dany delete
credentials = '/Volumes/Data/sling-stream-recover-eb60c169ef8a.json'
projectid = 'sling-stream-recover'

bqconn = db_connection.big_query(credentials, projectid)


def delete_data(uuid):
    lines = []
    queries = []
    with open("bqtables.txt") as file_in:

        for line in file_in:
            lines.append(line.strip())

    with open('lisquery.txt') as file:
        for quer in file:
            queries.append(quer.strip())

        for i in lines:
            super_logger.info(f'*****************{i}**********************')
            print('*****************{i}**********************')

            for j in queries:
                selectstatement = ' '.join(j.split()[1:2])
                query = str(j).format(bqschema.format(str(i)),
                                      "'" + uuid + "'") + ';'

                super_logger.info(f'BigQuery:{query}:')
                print('BigQuery:{query}:')


                # Get the result from Bq
                BQ_query = build_query.selectquery_bq(query, bqconn)
                result_BQ = BQ_query
                super_logger.info(f'BigQuery:{selectstatement}:{result_BQ}')
                print('BigQuery:{selectstatement}:{result_BQ}')


if __name__ == '__main__':
    delete_data("uuid")

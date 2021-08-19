from google.cloud import bigquery
from google.cloud import bigquery
from google.oauth2 import service_account


def uploadtobq(filename,bq_project,dataset_id,table_id):
    credentials = '/Volumes/Data/Tableau/CCPA/sling-studio-transfer-d377cc3410a8.json'

    credentials = service_account.Credentials.from_service_account_file(credentials)
    client = bigquery.Client(credentials=credentials, project=bq_project)

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 0
    job_config.autodetect = False

    print(filename)


    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        print("**************************",job)


    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

if __name__ == '__main__':
    uploadtobq('filename','bq_project','dataset_id','table_id')

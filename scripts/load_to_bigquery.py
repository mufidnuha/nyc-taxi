from google.cloud import bigquery
from sqlalchemy import table

def gcs_to_bigquery(year, project, dataset):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_name = 'trip'
    table_id = f"{project}.{dataset}.{table_name}"
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)
    uri = f"gs://nyc_taxi_study-infinate/pq_clean/{year}/01/*.parquet"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

    for month in range(2,13):
        try:
            previous_rows = client.get_table(table_id).num_rows
            fmonth = "{:02d}".format(month)

            job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)
            uri = f"gs://nyc_taxi_study-infinate/pq_clean/{year}/{fmonth}/*.parquet"

            load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.

            destination_table = client.get_table(table_id)
            loaded_rows = destination_table.num_rows - previous_rows
            print("Loaded {} rows.".format(loaded_rows))
        except:
            print("File not found")

# if __name__=="__main__":
#     gcs_to_bigquery(2021)
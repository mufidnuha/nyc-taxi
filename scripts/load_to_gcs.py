import os
from google.cloud import storage

# def upload_blob(bucket_name, source_file_name, destination_blob_name):
#     """Uploads a file to the bucket."""
#     # bucket_name = "your-bucket-name"
#     # source_file_name = "local/path/to/file"
#     # destination_blob_name = "storage-object-name"

#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)

#     blob.upload_from_filename(source_file_name)

def local_to_gcs(year, bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    months = os.listdir(f'tmp/pq_clean/{year}')

    for month in months:
        folder_path = f"pq_clean/{year}/{month}"
        files = os.listdir(f'tmp/{folder_path}')
        #print(files)
        for file in files:
            src_file = f"tmp/{folder_path}/{file}"
            dest_file = f"{folder_path}/{file}"
            blob = bucket.blob(dest_file)
            blob.upload_from_filename(src_file)

# if __name__=="__main__":
#      local_to_gcs(2021, 'nyc_taxi_study-infinate')

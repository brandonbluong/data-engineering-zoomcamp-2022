import io
import os
import datetime
import time
import requests
import pandas as pd
import pyarrow
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']

# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")


def upload_to_gcs(bucket, local_file, object_name):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        start_time = time.time()

        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        if service == 'zones':
            folder_url = 'misc/'
            file_name = 'taxi_zone_lookup.csv'
        else:
            folder_url = service + '/'
            file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # read url into request response object
        request_url = init_url + folder_url + file_name
        r = requests.get(request_url)

        # read using pandas and convert to parquet
        if service == 'zones':
            df = pd.read_csv(io.BytesIO(r.content))
            file_name = file_name.replace('.csv', '.parquet')
        else:
            df = pd.read_csv(io.BytesIO(r.content), compression="gzip")
            file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")

        # delete local copy
        os.remove(f"{file_name}")
        print(f'Cleaned up: {file_name}')
        print("Iteration took: %.2f minutes" %((time.time() - start_time)/60))
        if service == 'zones':
            return

def main():
    web_to_gcs('2019', 'yellow')
    web_to_gcs('2020', 'yellow')
    web_to_gcs('2019', 'fhv')
    web_to_gcs('2019', 'zones')
    web_to_gcs('2019', 'green')
    web_to_gcs('2020', 'green')

if __name__ == "__main__":
    main()
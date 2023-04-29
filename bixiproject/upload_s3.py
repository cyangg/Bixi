import pandas as pd
import requests
import boto3
from botocore.exceptions import NoCredentialsError

# Set up S3 connection
s3 = boto3.client('s3',
                  aws_access_key_id='****',
                  aws_secret_access_key='****')
bucket_name = 'bixi-database-henry'

# Read JSON data from API URL into a Pandas DataFrame
def get_data_from_api(url):
    response = requests.get(url)
    data = response.json()
    return pd.json_normalize(data['data']['stations'])

# Upload CSV data to S3 bucket
def upload_to_s3(df, object_name):
    csv_data = df.to_csv(index=False)
    try:
        s3.put_object(Body=csv_data, Bucket=bucket_name, Key=object_name)
        print(f"Upload successful for {object_name}")
    except NoCredentialsError:
        print("Credentials not available")

# API URLs
url1 = 'https://gbfs.velobixi.com/gbfs/en/station_information.json'
url2 = 'https://gbfs.velobixi.com/gbfs/en/station_status.json'

# Get data from APIs
df1 = get_data_from_api(url1)
df2 = get_data_from_api(url2)

# Upload data to S3
upload_to_s3(df1, 'station_information.csv')
upload_to_s3(df2, 'station_status.csv')





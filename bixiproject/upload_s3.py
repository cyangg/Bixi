import pandas as pd
import requests
import boto3
from botocore.exceptions import NoCredentialsError

# Set up S3 connection
s3 = boto3.client('s3',
                  aws_access_key_id='******',
                  aws_secret_access_key='**********')
bucket_name = 'bixi-database-henry'
object_name = 'station_information.csv'

# Read JSON data from API URL into a Pandas DataFrame
url = 'https://gbfs.velobixi.com/gbfs/en/station_information.json'
response = requests.get(url)
data = response.json()
df = pd.json_normalize(data['data']['stations'])

# Convert DataFrame to CSV format
csv_data = df.to_csv(index=False)

# Upload CSV data to S3 bucket
try:
    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=object_name)
    print("Upload successful")
except NoCredentialsError:
    print("Credentials not available")




import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Read the CSV files into Pandas dataframes with the correct encoding
station_status_df = pd.read_csv('station_status.csv', encoding='ISO-8859-1')
station_information_df = pd.read_csv('station_information.csv', encoding='ISO-8859-1')

# Write the dataframes to Parquet files
station_status_df.to_parquet('station_status.parquet')
station_information_df.to_parquet('station_information.parquet')






import pandas as pd


# Read the CSV files into Pandas dataframes
station_status_df = pd.read_csv('station_status.csv', encoding='ISO-8859-1')
station_information_df = pd.read_csv('station_information.csv', encoding='ISO-8859-1')

# Write the dataframes to Avro files
station_status_df.to_orc('station_status.orc')
station_information_df.to_orc('station_information.orc')

















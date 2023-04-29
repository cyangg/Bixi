import pandas as pd
import pandavro as pav

# Read the CSV files into Pandas dataframes
station_status_df = pd.read_csv('station_status.csv', encoding='ISO-8859-1')
station_information_df = pd.read_csv('station_information.csv', encoding='ISO-8859-1')

# Write the dataframes to Avro files
pav.to_avro('station_status.avro', station_status_df)
pav.to_avro('station_information.avro', station_information_df)







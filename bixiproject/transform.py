import psycopg2
import os
import pandas as pd
import pandavro as pav

# PostgreSQL database connection parameters
db_host = 'localhost'
db_port = '5432'
db_name = 'bixi'
db_user = 'postgres'
db_password = 'ch515254'

# Directory to store CSV files
csv_dir = 'C:/Users/cyang/bixiproject/'

def transform():
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )

    # Get list of tables in the database
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
    tables = cur.fetchall()

    # Export each table to a CSV file
    for table in tables:
        table_name = table[0]
        csv_file = os.path.join(csv_dir, f'{table_name}.csv')
        with open(csv_file, 'w') as f:
            cur.copy_expert(f"COPY {table_name} TO STDOUT WITH (FORMAT csv, HEADER true, DELIMITER ',')", f)
        print(f'{table_name} exported to {csv_file}')

    # Close database connection
    cur.close()
    conn.close()

    # Read the CSV files into Pandas dataframes
    station_status_df = pd.read_csv('station_status.csv', encoding='ISO-8859-1')
    station_information_df = pd.read_csv('station_information.csv', encoding='ISO-8859-1')

    # Write the dataframes to Avro files
    pav.to_avro('station_status.avro', station_status_df)
    pav.to_avro('station_information.avro', station_information_df)
    # Write the dataframes to Parquet files
    station_status_df.to_parquet('station_status.parquet')
    station_information_df.to_parquet('station_information.parquet')
    # Write the dataframes to ORC format
    station_status_df.to_orc('station_status.orc')
    station_information_df.to_orc('station_information.orc')

    print("Transformation completed.")


if __name__ == '__main__':
    transform()




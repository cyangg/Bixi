import psycopg2
import os

# PostgreSQL database connection parameters
db_host = 'localhost'
db_port = '5432'
db_name = 'bixi'
db_user = 'postgres'
db_password = 'ch515254'

# Directory to store CSV files
csv_dir = 'C:/Users/cyang/bixiproject/'

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


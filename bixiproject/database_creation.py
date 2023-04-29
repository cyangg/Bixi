import psycopg2

# Postgres SQL connection parameters
dbname = 'postgres'
user = 'postgres'
password = 'ch515254'
host = 'localhost'
port = '5432'

# Connect to Postgres SQL server
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# Create the 'bixi' database
conn.autocommit = True
cur = conn.cursor()
cur.execute('CREATE DATABASE bixi')
cur.close()

print('Database created successfully')

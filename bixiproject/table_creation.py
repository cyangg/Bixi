import requests
import json
import psycopg2
import datetime

# Postgres SQL connection parameters
dbname = 'bixi'
user = 'postgres'
password = 'ch515254'
host = 'localhost'
port = '5432'

# Connect to Postgres SQL database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# Create the station_information table if it doesn't already exist
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS station_information (
        station_id TEXT PRIMARY KEY,
        name TEXT,
        short_name TEXT,
        lat NUMERIC,
        lon NUMERIC,
        capacity INTEGER
    )
""")
conn.commit()

# Create the station_status table
cur = conn.cursor()
cur.execute('''
    CREATE TABLE station_status (
        station_id INTEGER,
        num_bikes_available INTEGER,
        num_ebikes_available INTEGER,
        num_bikes_disabled INTEGER,
        num_docks_available INTEGER,
        num_docks_disabled INTEGER,
        is_installed BOOLEAN,
        is_renting BOOLEAN,
        is_returning BOOLEAN,
        last_reported INTEGER,
        converted_last_reported TIMESTAMP
    )
''')

# Get data from the API
response = requests.get('https://gbfs.velobixi.com/gbfs/en/station_information.json')
if response.status_code == 200:
    data = json.loads(response.content)
    stations = data['data']['stations']

    # Insert the data into the station_information table
    cur = conn.cursor()
    for station in stations:
        cur.execute("""
            INSERT INTO station_information (station_id, name, short_name, lat, lon, capacity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (station_id) DO UPDATE
            SET name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                capacity = EXCLUDED.capacity
        """, 
        (station['station_id'], station['name'], station['short_name'], station['lat'], station['lon'], station['capacity']))
    conn.commit()
    cur.close()

# Get data from the API
response = requests.get('https://gbfs.velobixi.com/gbfs/en/station_status.json')
if response.status_code == 200:
    data = json.loads(response.content)
    stations = data['data']['stations']

    # Insert the data into the station_status table
    cur = conn.cursor()
    for station in stations:
        # Convert last_reported Unix timestamp to date and time
        converted_last_reported = datetime.datetime.fromtimestamp(station['last_reported'])
        # Cast is_installed and is_renting fields to boolean
        is_installed = bool(station['is_installed'])
        is_renting = bool(station['is_renting'])
        is_returning = bool(station['is_returning'])

        cur.execute("INSERT INTO station_status (station_id, num_bikes_available, num_ebikes_available, num_bikes_disabled, num_docks_available, num_docks_disabled, is_installed, is_renting, is_returning, last_reported, converted_last_reported) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                    (station['station_id'], station['num_bikes_available'], station['num_ebikes_available'], station['num_bikes_disabled'], station['num_docks_available'], station['num_docks_disabled'], is_installed, is_renting, is_returning, station['last_reported'], converted_last_reported))
    conn.commit()
    cur.close()


# Close the database connection
conn.close()



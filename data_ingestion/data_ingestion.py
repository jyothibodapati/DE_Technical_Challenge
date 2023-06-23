from os import environ
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
from time import sleep
import pandas as pd
import mysql.connector
import csv
import logging
import datetime

# Set up logging
log_file = f"/app/error_stg_event_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.log"
summary_file = f"/app/load_summary_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# MySQL database connection details
user = environ.get('MYSQL_USER', 'root')
password = environ.get('MYSQL_PASSWORD', 'root')
host = environ.get('MYSQL_HOST', 'mysqlinstapro-container')
database = environ.get('MYSQL_DATABASE_STG', 'stg_layer')

# Path to the CSV file
csv_file = '/app/event_log.csv'

# Print and log the CSV file path
logging.info(f"CSV file path: {csv_file}")

logging.info('Data ingestion started...')

max_retries = 10
retry_delay = 0.1

for _ in range(max_retries):
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(retry_delay)
else:
    # Executed if the loop completes without a successful connection
    logging.error('Failed to establish a connection to MySQL.')
    exit(1)

logging.info('Connection to MySQL successful.')

# Establish a connection to the MySQL database
connection = mysql.connector.connect(
    user=user,
    password=password,
    host=host,
    database=database
)
cursor = connection.cursor()

# Check if the table exists in the stg_layer schema
table_name = 'stg_event_log'
cursor.execute(f"SHOW TABLES IN {database} LIKE '{table_name}'")
table_exists = cursor.fetchone()

if not table_exists:
    logging.error(f"The table '{table_name}' does not exist in the database.")
    exit(1)

# Truncate the existing table
cursor.execute(f"TRUNCATE TABLE {database}.{table_name}")

# Load data from the CSV file into the table
loaded_rows = 0
error_count = 0

with open(csv_file, 'r') as file:
    reader = csv.reader(file, delimiter=';')
    next(reader)  # Skip header row

    for row in reader:
        try:
            if row[4] is None or row[4] == '':
                # Insert NULL values for additional columns
                insert_query = f"""
                INSERT INTO {database}.{table_name} (event_id, event_type, professional_id_anonymized, created_at, meta_data, service_id, service_name_nl, service_name_en, lead_fee, load_date)
                VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL, %s)
                """
                cursor.execute(insert_query, (row[0], row[1], row[2], row[3], datetime.datetime.now()))
            else:
                # Split the meta_data and extract values
                meta_data_parts = row[4].split('_')
                service_id = int(meta_data_parts[0])
                service_name_nl = meta_data_parts[1]
                service_name_en = meta_data_parts[2]
                lead_fee = float(meta_data_parts[3])

                insert_query = f"""
                INSERT INTO {database}.{table_name} (event_id, event_type, professional_id_anonymized, created_at, meta_data, service_id, service_name_nl, service_name_en, lead_fee, load_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (row[0], row[1], row[2], row[3], row[4], service_id, service_name_nl, service_name_en, lead_fee, datetime.datetime.now()))

            loaded_rows += 1
        except Exception as e:
            logging.error(f"Error inserting record: {row} - {str(e)}")
            error_count += 1

# Commit the changes and close the database connection
connection.commit()
cursor.close()
connection.close()

logging.info('Data ingestion completed.')
logging.info(f"Loaded rows: {loaded_rows}")
logging.info(f"Errors encountered: {error_count}")

with open(summary_file, 'w') as summary:
    summary.write(f"Loaded rows: {loaded_rows}\n")
    summary.write(f"Errors encountered: {error_count}\n")

print("Data inserted into the 'stg_event_log' table in MySQL.")
print(f"Error log file created: {log_file}")
print(f"Load summary file created: {summary_file}")

logging.info('STG Load Completed.')
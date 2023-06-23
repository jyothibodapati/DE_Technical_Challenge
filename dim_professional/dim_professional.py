from os import environ
import mysql.connector
from datetime import datetime

# Database connection details
user = environ.get('MYSQL_USER', 'root')
password = environ.get('MYSQL_PASSWORD', 'root')
host = environ.get('MYSQL_HOST', 'mysqlinstapro-container')
database = environ.get('MYSQL_DATABASE_ANALYTICS', 'analytics_layer')

# Establishing connection to the database
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Creating a cursor object
cursor = connection.cursor()

# Select data from the staging table
select_query = "SELECT distinct professional_id_anonymized FROM stg_layer.stg_event_log"
cursor.execute(select_query)
results = cursor.fetchall()

# Initialize counters
total_rows = len(results)
updated_rows = 0
inserted_rows = 0

# Insert or update data in the dim_professional table
for result in results:
    professional_id_anonymized = result[0]
    if professional_id_anonymized is None:
        continue

    load_date = datetime.now()

    # Check if the professional exists in the dim_professional table
    check_query = f"SELECT professional_id_anonymized FROM dim_professional WHERE professional_id_anonymized = {professional_id_anonymized}"
    cursor.execute(check_query)
    existing_row = cursor.fetchone()

    if existing_row and existing_row[0] != professional_id_anonymized:
        # Professional exists and value has changed, update the load_date
        update_query = f"UPDATE dim_professional SET LOAD_DATE = '{load_date}' WHERE professional_id_anonymized = {professional_id_anonymized}"
        cursor.execute(update_query)
        updated_rows += 1
    elif not existing_row:
        # Professional doesn't exist, insert a new row
        insert_query = f"INSERT INTO dim_professional (professional_id_anonymized, LOAD_DATE) VALUES ({professional_id_anonymized}, '{load_date}')"
        cursor.execute(insert_query)
        inserted_rows += 1

# Commit the changes
connection.commit()

# Close the connection
connection.close()

# Print summary
print("DIM Professional Load Summary")
print("-----------------------------")
print(f"Total Rows: {total_rows}")
print(f"Updated Rows: {updated_rows}")
print(f"Inserted Rows: {inserted_rows}")
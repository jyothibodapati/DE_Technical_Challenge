from os import environ
import mysql.connector
from datetime import datetime, timedelta

# Database connection details
user = environ.get('MYSQL_USER', 'root')
password = environ.get('MYSQL_PASSWORD', 'root')
host = environ.get('MYSQL_HOST', 'mysqlinstapro-container')
database = environ.get('MYSQL_DATABASE_ANALYTICS', 'analytics_layer')

# Define the minimum and maximum dates
min_date = '2020-01-01'
max_date = '2020-03-10'

# Establishing connection to the database
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Creating a cursor object
cursor = connection.cursor()

# Truncate the fact table before loading data
truncate_query = "TRUNCATE TABLE fct_availability_snapshot"
cursor.execute(truncate_query)

# Generate a list of dates within the range
dates = []
current_date = datetime.strptime(min_date, '%Y-%m-%d').date()
max_date = datetime.strptime(max_date, '%Y-%m-%d').date()
while current_date <= max_date:
    dates.append(current_date)
    current_date += timedelta(days=1)

# Initialize counters
total_dates = len(dates)
inserted_rows = 0

# Loop through each date and calculate the active professionals count
for event_date in dates:
    # Count the number of active professionals for the current date
    count_query = f"""
        SELECT COUNT(DISTINCT professional_id_anonymized)
        FROM stg_layer.stg_event_log
        WHERE DATE(created_at) <= '{event_date}' AND event_type = 'became_able_to_propose'
            AND professional_id_anonymized NOT IN (
                SELECT professional_id_anonymized
                FROM stg_layer.stg_event_log
                WHERE DATE(created_at) <= '{event_date}' AND event_type = 'became_not_able_to_propose'
            )
    """
    cursor.execute(count_query)
    active_professionals_count = cursor.fetchone()[0]

    # Insert the data into the fact table
    insert_query = f"""
        INSERT INTO fct_availability_snapshot (date_key, active_professionals_count)
        VALUES ((SELECT date_key FROM dim_date WHERE date_value = '{event_date}'), {active_professionals_count})
    """
    cursor.execute(insert_query)
    inserted_rows += 1

# Commit the changes
connection.commit()

# Close the connection
connection.close()

# Print summary
print("FCT Availability Snapshot Load Summary")
print("--------------------------------------")
print(f"Total Dates: {total_dates}")
print(f"Inserted Rows: {inserted_rows}")
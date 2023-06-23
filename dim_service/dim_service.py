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

# Select distinct data from the staging table
select_query = """
    SELECT DISTINCT
        service_id,
        service_name_nl,
        service_name_en
    FROM
        stg_layer.stg_event_log where service_id is not null
"""
cursor.execute(select_query)
results = cursor.fetchall()

# Initialize counters
total_rows = len(results)
updated_rows = 0
inserted_rows = 0

# Loop through each row in the result set
for row in results:
    service_id = row[0]
    service_name_nl = row[1]
    service_name_en = row[2]

    # Check if the record exists in the dim_service table
    check_query = f"SELECT service_name_nl, service_name_en FROM dim_service WHERE service_id = {service_id}"
    cursor.execute(check_query)
    existing_row = cursor.fetchone()

    if existing_row is not None:
        # Compare the existing values with the new values
        existing_service_name_nl, existing_service_name_en = existing_row

        if (existing_service_name_nl != service_name_nl) or (existing_service_name_en != service_name_en):
            # Update the record if there is a change
            update_query = f"""
                UPDATE dim_service
                SET service_name_nl = '{service_name_nl}',
                    service_name_en = '{service_name_en}',
                    load_date = NOW()
                WHERE service_id = {service_id}
            """
            cursor.execute(update_query)
            updated_rows += 1
        else:
            # No change in the values, skip updating
            continue
    else:
        # Insert the record if it doesn't exist
        insert_query = f"""
            INSERT INTO dim_service (service_id, service_name_nl, service_name_en, load_date)
            VALUES ({service_id}, '{service_name_nl}', '{service_name_en}', NOW())
        """
        cursor.execute(insert_query)
        inserted_rows += 1


# Commit the changes
connection.commit()

# Close the connection
connection.close()

# Print summary
print("DIM Service Load Summary")
print("-----------------------")
print(f"Total Rows: {total_rows}")
print(f"Updated Rows: {updated_rows}")
print(f"Inserted Rows: {inserted_rows}")
from os import environ
import mysql.connector
from datetime import datetime, timedelta

# MySQL database connection details
user = environ.get('MYSQL_USER', 'root')
password = environ.get('MYSQL_PASSWORD', 'root')
host = environ.get('MYSQL_HOST', 'mysqlinstapro-container')
database = environ.get('MYSQL_DATABASE_ANALYTICS', 'analytics_layer')

# MySQL connection details
config = {
    'user': user,
    'password': password,
    'host': host,
    'database': database,
    'raise_on_warnings': True
}

# Function to insert a single date record into the dimension table
def insert_date(cursor, date):
    query = "INSERT INTO dim_date (date_key, date_value, day_of_week, day_name, month_name, year, load_date) " \
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (
        date.strftime('%Y%m%d'),
        date.strftime('%Y-%m-%d'),
        date.strftime('%w'),
        date.strftime('%A'),
        date.strftime('%B'),
        date.year,
        datetime.now()
))

# Function to load the date dimension table
def load_date_dimension(start_date, end_date):
    try:
        # Connect to MySQL
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()

        # Delete existing records in the dimension table
        cursor.execute("DELETE FROM dim_date")

        # Generate and insert date records
        current_date = start_date
        while current_date <= end_date:
            insert_date(cursor, current_date)
            current_date += timedelta(days=1)

        # Commit the changes and close the cursor and connection
        cnx.commit()
        cursor.close()
        cnx.close()

        print("Date dimension loaded successfully.")

    except mysql.connector.Error as err:
        print("An error occurred:", err)


# Specify the start and end dates
start_date = datetime(2020, 1, 1)
end_date = datetime.now()

# Load the date dimension
load_date_dimension(start_date, end_date)

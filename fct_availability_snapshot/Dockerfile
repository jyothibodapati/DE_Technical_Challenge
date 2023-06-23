FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy mysql-connector-python pandas pymysql
WORKDIR /app
COPY . .
CMD [ "python", "./fct_availability_snapshot.py" ]
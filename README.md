## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create an empty MySQL database along with the following data ingestion and transformation pipeline containers.

docker_instapro-data_ingestion
docker_instapro-dim_date 
docker_instapro-dim_professional
docker_instapro-dim_service
docker_instapro-fct_availability_snapshot

Run the SQLs in the deployment.sql document by connecting to the mysql database from CLI.
/*mysql>
C:\Users\Yuvi>docker exec -it mysqlinstapro-container mysql -u root -p -D information_schema
Enter password:root */
Once the tables are created and necessary grants are provided, restart the following containers in the same order.
docker_instapro-data_ingestion
docker_instapro-dim_date 
docker_instapro-dim_professional
docker_instapro-dim_service
docker_instapro-fct_availability_snapshot

Note: Incase if you face any errors while connecting to the mysql database, please wait a couple of minutes and make sure that the db container is up and running and then restart the data ingestion and transformation pipeline containers.


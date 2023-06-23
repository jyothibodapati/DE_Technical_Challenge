/*STG deployment - Data Ingestion*/

CREATE DATABASE stg_layer;

USE stg_layer;


CREATE TABLE stg_event_log (
    event_id INT,
    event_type VARCHAR(255),
    professional_id_anonymized INT,
    created_at DATETIME,
    meta_data VARCHAR(255),
    service_id INT,
    service_name_nl VARCHAR(255),
    service_name_en VARCHAR(255),
    lead_fee FLOAT,
    load_date DATETIME
);

/*Transformation layer deployment- Dimensions and Facts*/

CREATE DATABASE analytics_layer;

USE analytics_layer;

/*Create the dimension tables*/
CREATE TABLE dim_date (
  date_key INT PRIMARY KEY,
  date_value DATE,
  day_of_week INT,
  day_name VARCHAR(20),
  month_name VARCHAR(20),
  year INT,
  load_date TIMESTAMP,
  INDEX idx_dim_date_key (date_key)
);


CREATE TABLE dim_professional (
  P_SKEY INT AUTO_INCREMENT PRIMARY KEY,
  professional_id_anonymized INT,
  LOAD_DATE TIMESTAMP,
  INDEX idx_professional_id_anonymized (professional_id_anonymized)
);


CREATE TABLE dim_service (
  s_skey INT PRIMARY KEY AUTO_INCREMENT,
  service_id INT,
  service_name_nl VARCHAR(255),
  service_name_en VARCHAR(255),
  load_date TIMESTAMP,
  INDEX idx_service_id (service_id),
  INDEX idx_service_name_nl (service_name_nl),
  INDEX idx_service_name_en (service_name_en)
);

/*Create the fact tables*/
CREATE TABLE fct_availability_snapshot (
  date_key INT,
  active_professionals_count INT,
  FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
  INDEX idx_date_key (date_key)
);

/*grant privileges for nonroot user */
GRANT ALL PRIVILEGES ON stg_layer.* TO 'nonroot';

GRANT ALL PRIVILEGES ON analytics_layer.* TO 'nonroot';


/*mysql>
C:\Users\Yuvi>docker exec -it mysqlinstapro-container mysql -u root -p -D information_schema
Enter password:root */
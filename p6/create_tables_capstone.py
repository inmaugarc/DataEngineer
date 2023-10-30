CREATE TABLE Staging_Temperature(
    dt                            TIMESTAMP,
    AverageTemperature            DECIMAL(16,4),
    AverageTemperatureUncertainty DECIMAL(16,2),
    City                          VARCHAR(256),
    Country                       VARCHAR(256),
    Latitude                      VARCHAR(256),
    Longitude                     VARCHAR(256)
);

CREATE TABLE Staging_AirQuality(
    city                        VARCHAR(256),
    coordinates                 VARCHAR(256),
    country                     VARCHAR(256),
    country_name_en             VARCHAR(256),
    location                    VARCHAR(256),
    measurements_lastupdated    VARCHAR(256),
    measurements_parameter      VARCHAR(256),
    measurements_sourcename     VARCHAR(256),
    measurements_unit           VARCHAR(256),
    measurements_value          DECIMAL(16,4)
);

CREATE TABLE Staging_Electric_Vehicles_Sellings(
    region      VARCHAR(256),
    category    VARCHAR(256),
    parameter   VARCHAR(256),
    mode        VARCHAR(256)),
    powertrain  VARCHAR(256),
    year        INT,
    unit        VARCHAR(256),
    value       DECIMAL(16,4)
);

CREATE TABLE Staging_WorldPopulation(
    Rank                        INT,
    CCA3                        INT,
    Country                     VARCHAR(256),
    Capital                     INT,
    Continent                   VARCHAR(256),
    2022_Population             INT,
    2020_Population             INT,
    2015_Population             INT,
    2010_Population             INT,
    2000_Population             INT,
    1990_Population             INT,
    1980_Population             INT,
    1970_Population             INT,
    Area                        DECIMAL(16,4),
    Density                     DECIMAL(16,4),
    Growth_Rate                 DECIMAL(16,4),
    World_Population_Percentage DECIMAL(16,4)    
);

CREATE TABLE dim_country(
    country_id      SERIAL PRIMARY KEY,
    country         VARCHAR(256),
    country_name_en VARCHAR(256),
    CCA3            VARCHAR(256),               
    capital         VARCHAR(256),
    continent       VARCHAR(256),
    area            INT    
);

CREATE TABLE dim_datetime (
    dt TIMESTAMP NOT NULL PRIMARY KEY,    
    hour    INT NOT NULL,
    day     INT NOT NULL,
    week    INT NOT NULL,
    month   INT NOT NULL,
    year    INT NOT NULL,
    weekday INT NOT NULL
);    

CREATE TABLE dim_vehicle (
    mode        VARCHAR(256) NOT NULL PRIMARY_KEY,
    powertrain  VARCHAR(256)
);

CREATE TABLE fact_temperature(
    country_id INT NOT NULL PRIMARY KEY, 
    dt TIMESTAMP NOT NULL PRIMARY KEY,
    city VARCHAR(256) NOT NULL PRIMARY KEY, 
    averageTemperature DECIMAL(16,4),
    averageTemperatureUncertainty DECIMAL(16,4),
    latitude VARCHAR(256),
    longitude VARCHAR(256),
    year INT,
    CONSTRAINT fk_country
      FOREIGN KEY(country_id) 
      REFERENCES dim_country(country_id),
    CONSTRAINT fk_dt
      FOREIGN KEY(dt) 
      REFERENCES Dim_Datetime(dt)  
);    

CREATE TABLE fact_population(
    country_id INT NOT NULL PRIMARY_KEY,
    city VARCHAR(216),
    rank INT,
    2022_Population INT,
    2020_Population INT,
    2010_Population INT,
    2000_Population INT,
    1990_Population INT,
    1980_Population INT,
    1970_Population INT,
    density FLOAT,
    growthRate FLOAT,
    worldPopulationPercentage FLOAT,
    CONSTRAINT fk_country
      FOREIGN KEY(country_id) 
      REFERENCES dim_country(country_id)
);  

CREATE TABLE fact_airquality(
    country_id INT NOT NULL PRIMARY_KEY,
    city VARCHAR(216),
    dt TIMESTAMP NOT NULL PRIMARY_KEY,
    location VARCHAR(216) NOT NULL PRIMARY_KEY,
    measurements_parameter VARCHAR(216),
    measurements_unit VARCHAR(216),
    measurements_value FLOAT;
    measurements_lastupdated VARCHAR(216),
    measurements_sourcename VARCHAR(216),
    year INT,
    CONSTRAINT fk_country
      FOREIGN KEY(country_id) 
      REFERENCES dim_country(country_id),
    CONSTRAINT fk_dt
      FOREIGN KEY(dt) 
      REFERENCES Dim_Datetime(dt)      
);

CREATE TABLE fact_car_sales(
    country_id INT NOT NULL PRIMARY_KEY,
    mode VARCHAR(216) NOT NULL PRIMARY_KEY,
    year INT,
    parameter VARCHAR(216),
    powertrain VARCHAR(216) NOT NULL PRIMARY_KEY, 
    unit VARCHAR(216),
    value FLOAT,
    category VARCHAR(216), 
    CONSTRAINT fk_country
      FOREIGN KEY(country_id) 
      REFERENCES dim_country(country_id),
    CONSTRAINT fk_mode
      FOREIGN KEY(mode) 
      REFERENCES Dim_Vehicle(mode), 
    CONSTRAINT fk_powertrain
      FOREIGN KEY(powertrain) 
      REFERENCES Dim_Vehicle(powertrain)
);
    
    
####################
CREATE_TRIPS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trips (
trip_id INTEGER NOT NULL,
start_time TIMESTAMP NOT NULL,
end_time TIMESTAMP NOT NULL,
bikeid INTEGER NOT NULL,
tripduration DECIMAL(16,2) NOT NULL,
from_station_id INTEGER NOT NULL,
from_station_name VARCHAR(100) NOT NULL,
to_station_id INTEGER NOT NULL,
to_station_name VARCHAR(100) NOT NULL,
usertype VARCHAR(20),
gender VARCHAR(6),
birthyear INTEGER,
PRIMARY KEY(trip_id))
DISTSTYLE ALL;
"""

CREATE_STATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stations (
id INTEGER NOT NULL,
name VARCHAR(250) NOT NULL,
city VARCHAR(100) NOT NULL,
latitude DECIMAL(9, 6) NOT NULL,
longitude DECIMAL(9, 6) NOT NULL,
dpcapacity INTEGER NOT NULL,
online_date TIMESTAMP NOT NULL,
PRIMARY KEY(id))
DISTSTYLE ALL;
"""

COPY_SQL = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{{}}'
SECRET_ACCESS_KEY '{{}}'
IGNOREHEADER 1
DELIMITER ','
"""

COPY_MONTHLY_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/partitioned/{year}/{month}/divvy_trips.csv"
)

COPY_ALL_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

COPY_STATIONS_SQL = COPY_SQL.format(
    "stations",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
)

LOCATION_TRAFFIC_SQL = """
BEGIN;
DROP TABLE IF EXISTS station_traffic;
CREATE TABLE station_traffic AS
SELECT
    DISTINCT(t.from_station_id) AS station_id,
    t.from_station_name AS station_name,
    num_departures,
    num_arrivals
FROM trips t
JOIN (
    SELECT
        from_station_id,
        COUNT(from_station_id) AS num_departures
    FROM trips
    GROUP BY from_station_id
) AS fs ON t.from_station_id = fs.from_station_id
JOIN (
    SELECT
        to_station_id,
        COUNT(to_station_id) AS num_arrivals
    FROM trips
    GROUP BY to_station_id
) AS ts ON t.from_station_id = ts.to_station_id
"""

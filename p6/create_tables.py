CREATE TABLE IF NOT EXISTS public.Staging_Temperature(
    dt                            TIMESTAMP,
    AverageTemperature            FLOAT,
    AverageTemperatureUncertainty FLOAT,
    City                          VARCHAR(256),
    Country                       VARCHAR(256),
    Latitude                      VARCHAR(256),
    Longitude                     VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS public.Staging_AirQuality(
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

CREATE TABLE IF NOT EXISTS public.Staging_Electric_Vehicles_Sellings(
    region      VARCHAR(256),
    category    VARCHAR(256),
    parameter   VARCHAR(256),
    mode        VARCHAR(256)),
    powertrain  VARCHAR(256),
    year        INT,
    unit        VARCHAR(256),
    value       DECIMAL(16,4)
);

CREATE TABLE IF NOT EXISTS public.Staging_WorldPopulation(
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

CREATE TABLE IF NOT EXISTS public.dim_country(
    country_id      SERIAL PRIMARY KEY,
    country         VARCHAR(256),
    country_name_en VARCHAR(256),
    CCA3            VARCHAR(256),               
    capital         VARCHAR(256),
    continent       VARCHAR(256),
    area            INT    
)diststyle key distkey(country_id);

CREATE TABLE IF NOT EXISTS public.dim_datetime (
    dt TIMESTAMP NOT NULL SORTKEY,    
    hour    INT NOT NULL,
    day     INT NOT NULL,
    week    INT NOT NULL,
    month   INT NOT NULL,
    year    INT NOT NULL,
    weekday INT NOT NULL
)primary key(dt);    

CREATE TABLE IF NOT EXISTS public.dim_vehicle (
    mode        VARCHAR(256) NOT NULL PRIMARY_KEY,
    powertrain  VARCHAR(256)
) diststyle key distkey(mode);

CREATE TABLE IF NOT EXISTS public.fact_temperature(
    country_id INT NOT NULL PRIMARY KEY, 
    dt TIMESTAMP NOT NULL PRIMARY KEY,
    city VARCHAR(256) NOT NULL PRIMARY KEY, 
    averageTemperature FLOAT,
    averageTemperatureUncertainty FLOAT,
    latitude VARCHAR(256),
    longitude VARCHAR(256),
    year INT,
    CONSTRAINT fk_country
      FOREIGN KEY(country_id) 
      REFERENCES dim_country(country_id),
    CONSTRAINT fk_dt
      FOREIGN KEY(dt) 
      REFERENCES Dim_Datetime(dt)  
)
diststyle all;    

CREATE TABLE IF NOT EXISTS public.fact_population(
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
)diststyle all;  

CREATE TABLE IF NOT EXISTS public.fact_airquality(
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
)diststyle all;

CREATE TABLE IF NOT EXISTS public.fact_car_sales(
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
)diststyle all;

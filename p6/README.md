# DATA ENGINEER MASTER
# Data Pipelines with Apache Airflow
A Udacity Data Engineer Nanodegree Project
  
### Table of Contents

1. [Project Motivation](#motivation)
2. [Project Structure](#structure)
3. [Source Datasets](#source_datasets)
4. [Schema Design](#schema)
5. [Data dictionary](#dictionary)
6. [Technologies](#tech)
7. [Data Pipeline](#pipeline)
8. [DAGs](#dags)
9. [Data Quality Checks](#quality)
10. [Scenarios](#scenarios)
11. [Graphics](#graphics)     
12. [Licensing, Authors, and Acknowledgements](#licensing)
13. [References](#references)


## Project Motivation<a name="motivation"></a> 

The objective of this project is to build an analytics database for detecting relation between temperature and air quality, demographic features and electric vehicles sellings all over the world and specifically in Europe.
This database will be helpful to find answers for questions such as:

* How has weather and air quality in Europe changed over the years?

* What are the countries with more electric vehicles? Has the air a better quality?

* What is the country with highest longevity? Is there a relation between the air quality? Is there a relation between air quality and human longevity?

##  Project Structure<a name="structure"></a> 

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data

## Source Datasets <a name="source_datasets"></a>

> * There are 4 source datasets, found in public domains:
> * GlobalLandTemperatures:
    Source: Udacity
    File format: CSV
    More than 8 million lines
  
> * Air Quality
    Source: Opendatasoft
    File format:JSON

> * Electric Vehicles Sellings
    Source: IEA
    File format: CSV

> * World Population Data
    Source: https://www.worlddata.info/
    File format: CSV

## Schema Design <a name="schema"></a>

The schema of the Database is a star schema and can be described with the following diagram:
![Alt text](./img/capstone_db.png?raw=true "Database_model")


> * The tables on the left (with the title in grey) contain data load from the source datasets.
> * The tables on the right are the Fact and Dimensional tables. Fact tables are coloured with green, dimensional tables are coloured with blue.
> * There are 4 staging tasks to extract information from the original source files
> * After that, we will load into 4 fact tables and 3 dimensional tables
> * During the process we run several data quality checks to test that everything went correct
> * In tables with a year column, this column is used as the sort key to improve the performance of the year-based searches

## Data dictionary <a name="dictionary"></a>

 - [x] Global Land Temperatures by city
   This dataset contains 7 columns
    -  dt: datetime it shows the date of the observation
    -  AverageTemperature: The average temperature of the observation
    -  AverageTemperatureUncertainty
    -  City: The city where the temperature observation was taken
    -  Country: The Country where the temperature observation was taken
    -  Latitude: The Latitude where the temperature observation was taken
    -  Longitude: The Longitude where the temperature observation was taken
- [x] World Population Dataset
  We have a dataset with 234 rows and 17 columns (variables) and no nulls
    -  Rank: Rank by population
    -  CCA3: 3 digit Country/Territories code
    -  Country: Name of the Country/Territories
    -  Capital: Name of the Capital
    -  Continent: Name of the Continent
    -  2022 Population: Population of the Country/Territories in the year 2022
    -  2020 Population: Population of the Country/Territories in the year 2020
    -  2015 Population: Population of the Country/Territories in the year 2015
    -  2010 Population: Population of the Country/Territories in the year 2010
    -  2000 Population: Population of the Country/Territories in the year 2000
    -  1990 Population: Population of the Country/Territories in the year 1990
    -  1980 Population: Population of the Country/Territories in the year 1980
    -  1970 Population: Population of the Country/Territories in the year 1970
    -  Area (km²): Area size of the Country/Territories in square kilometer
    -  Density (per km²): Population density per square kilometer
    -  Growth Rate: Population growth rate by Country/Territories
    -  World Population Percentage: The population percentage by each Country/Territories

  - [x] Dataset Air Quality by city
    This dataset contains 10 columns that describe the air quality of different cities in the world
    -  city: City where the air quality has been measured
    -  coordinates: Exact location
    -  country: Country                     
    -  country_name_en: Abbreviations in english for the country name             
    -  location: Location of the city                    
    -  measurements_lastupdated: When the measurements have been made
    -  measurements_parameter: Air Quality parameter that have been measured 
    -  measurements_sourcename: Entity that takes the measurements     
    -  measurements_unit: Unit for that measurement        
    -  measurements_value: Value for that measurement

  - [x] Electric Vehicle Sellings
 This dataset contains 8 columns that shows the number of sellings of electric vehicles
    -  region: Region of the world
    -  category: Category of the information ('Historical', 'Projection-STEPS', 'Projection-APS)
    -  parameter: EV Sales/EV stock/EV stock share/EV sales share/EV charging points/Oil displacement Mbd/Oil displacement Mlge/Electricity demand
    -  mode: 
    -  powertrain: For vehicles: BEV/EV/PHEV, And for the charging points: Publicly available fast/Publicly available slow
    -  year: Year of the selling 
    -  unit: sales/stock/percent/charging points/Milion barrels per day/Milion litres gasoline equivalent/GWh
    -  value:  Sold Units

## Technologies<a name="tech"></a> 
This project makes use of several Big Data techniques and tools such as:
* Pandas was used to explore the datasets since it has an easy and fast API
* Matplotlib as an easy way to visualize graphically and deduce insights from data
* Apache Spark as the most popular framework for distributed data processing for manipulating data at scale, in-memory data caching, and reuse across computations with high speed, ease of use and rich API

## Data Pipeline<a name="pipeline"></a> 

The steps to process the data are the following:
1. Load data into staging tables
2. Create dimension tables
3. Create facts table
4. Export data into parquet files
5. Perform several data quality checks (number of rows, query the tables, etc..)

## Data Quality Checks<a name="quality"></a> 
There are some Data Quality Checks included in this code: for checking the number of rows when we read the spark files written with the ETL and several queries to check that everything is ok

## Scenarios <a name="scenarios"></a> 
If the data was increased by 100x: We would use different approach for data storage such as Amazon EMR, Apache Cassandra, Amazon Athena or AWS Redshift and adapting the resources or even using a serverless Redshift so that we could forget about managing those resources
The data populates a dashboard that must be updated on a daily basis by 7am every day: In this case we could use Apache Airflow to manage the ETL, set specific tasks to be executed periodically including quality checks either on the data as well as on the overall process and setting alerts and notifications. Also we could use AWS Glue with crawlers to manage the ETL and perform periodical tasks.
The database needed to be accessed by 100+ people: For this purpose I would use AWS Redshift and adjust resources depending on the type of accesses (if people has to access simultaneously or not)

## Graphics <a name="graphics"></a> 
Here some graphics to a better visualization of data 
![Alt text](./img/Locations_BCN.png?raw=true "Locations of measurements in Barcelona")
![Alt text](./img/ev_sales_world.png?raw=true "Sales of Electric Vehicles in the world along time")
![Alt text](./img/ev_sales.png?raw=true "Sales of Electric Vehicles in Spain along time")
![Alt text](./img/top_populated.png?raw=true "Top 10 Most Populated Countries")
![Alt text](./img/top_empty.png?raw=true "Top 10 Least Populated Countries")


## Licensing, Authors, Acknowledgements<a name="licensing"></a>

Must give credit to Udacity for collecting data and because there are some pieces of code taken out from the Data Engineer Nanodegree classrooms. 
Also credits to Udacity Knowledge, where there is important information to develop the project.
And credits to Stackoverflow as it has been a useful source to solve some errors

## References <a name="references"></a>
 [The Power of Spark](https://learn.udacity.com/nanodegrees/nd027/parts/cd0030/lessons/ls1965/concepts/626aa254-50bc-4bc7-8fe9-9a4e28527739) <br>
 [Udacity Knowledge](https://knowledge.udacity.com/) <br>
 [StackOverflow](https://stackoverflow.com/) <br>
 [pySparkTutorial](https://github.com/roshankoirala/pySpark_tutorial)<br>
 
 


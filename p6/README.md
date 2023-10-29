# DATA ENGINEER MASTER
# Data Pipelines with Apache Airflow
A Udacity Data Engineer Nanodegree Project
  
### Table of Contents

1. [Project Motivation](#motivation)
2. [Project Structure](#structure)
3. [Source Datasets](#source_datasets)
4. [Schema Design](#schema)
5. [Data dictionary](#dictionary)
6. [Dashboards](#dash)
7. [Licensing, Authors, and Acknowledgements](#licensing)
8. [References](#references)


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
>* World Population Dataset
We have a dataset with 234 rows and 17 columns (variables) and no nulls 
 - Rank: Rank by population
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

## Dashboarding<a name="dash"></a> 

During the project, I have also added an EDA (Exploratory Data Analysis) and I include here some of the graphics for a better understanding of this dataset
<br>

 <br>Users by level
![Alt text](./img/level.png?raw=true "UsersbyLevel")

<br>Level of users' accounts
![Alt text](./img/level_plot.png?raw=true "Level account of Users")

<br>Graphic of Location of Users
![Alt text](./img/location.png?raw=true "Users by Location")

 <br>Histogram of the length users stay
![Alt text](./img/hist.png?raw=true "Histogram")

 <br>Skewness of data
![Alt text](./img/skewness.png?raw=true "Skewness")

## Licensing, Authors, Acknowledgements<a name="licensing"></a>

Must give credit to Udacity for collecting data and because there are some pieces of code taken out from the Data Engineer Nanodegree classrooms. 
Also credits to Udacity Knowledge, where there is important information to develop the project.
And credits to Stackoverflow as it has been a useful source to solve some errors
Some piece of code to plot data has been taken from https://github.com/roshankoirala/pySpark_tutorial

## References <a name="references"></a>
 [The Power of Spark](https://learn.udacity.com/nanodegrees/nd027/parts/cd0030/lessons/ls1965/concepts/626aa254-50bc-4bc7-8fe9-9a4e28527739) <br>
 [Udacity Knowledge](https://knowledge.udacity.com/) <br>
 [StackOverflow](https://stackoverflow.com/) <br>
 [pySparkTutorial](https://github.com/roshankoirala/pySpark_tutorial)<br>
 
 


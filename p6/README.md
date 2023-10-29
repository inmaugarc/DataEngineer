# DATA ENGINEER MASTER
# Data Pipelines with Apache Airflow
A Udacity Data Engineer Nanodegree Project
  
### Table of Contents

1. [Project Motivation](#motivation)
2. [Project Structure](#structure)
3. [Source Datasets](#source_datasets)
4. [Schema Design](#schema)
5. [File Descriptions](#files)
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


> * There are 2 staging tasks to extract information from the log events and songs tables from S3 JSON files
> * After that, we will load the songplays fact table
> * Then load into four dimensional tables: users, songs, artists, time
> * Finally we run several data quality checks to test that everything went correct

## File Descriptions <a name="files"></a>

These are the python scripts that create the databases schema and all the queries:

1. launch_emr.py: Create an EMR cluster on AWS cloud <br>
2. etl.py: Read the Json files and load that info into the created tables
3. destroy_cluster.py: This script destroys the EMR cluster

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
 
 


# DATA ENGINEER MASTER
# Data Modeling with Postgres
A Udacity Data Engineer Nanodegree Project

![Alt text](./img/music_petit.jpg?raw=true "A Project about music!!")

### Table of Contents

1. [Installation](#installation)
2. [Project Motivation](#motivation)
3. [File Descriptions](#files)
4. [Results](#results)
5. [Licensing, Authors, and Acknowledgements](#licensing)
6. [References](#references)

## Installation <a name="installation"></a>

There are some libraries that you need to install to run the code beyond the Anaconda distribution of Python. 
Those libraries are detailed in the requierements.txt file, so be sure that you meet all the requirements (by installing them with 'pip3 install requirements.txt').
The code should run with no issues using Python versions 3.* but if you find errors, please contact me.
Be the force with you!


## Project Motivation<a name="motivation"></a>

There is a startup named Sparkify that wants to analyze the data they have been collecting about the songs and other user activity on their new music streaming app. They want to answer to the following question:

> * Question 1: What are the songs that users are listening to?

They don't have an easy way to query their data that is stored in a directory of JSON files with the logs on user activity on the app. They have also a directory with JSON metadata on the songs in their app.

![Alt text](./img/question.jpg?raw=true "Question")

## File Descriptions <a name="files"></a>

There is a notebook available here to showcase work related to the above questions. The notebook is exploratory in searching through the data pertaining to the questions previously presented. Markdown cells & comments were used to assist in walking through the analysis process for individual steps.

There are 3 folders, one for year (2020, 2019 and 2018) containing each one a zip file with two files (I had to upload them zipped as they exceed the maximum file limits of Github): 

survey_results_public.csv       - This file contains the answers to the survey, it contains different kinds of variables either numerical or categorical. It contains missing values.
survey_results_schema.csv       - This file contains the questions of the survey

You have to unzip those files, after download them.

## Results<a name="results"></a>

The main findings of the code can be found at the post available [here](https://inmaug.medium.com/4-tips-that-will-make-you-smarter-about-developers-trends-around-the-globe-11c43a2e8536).

![Alt text](./img/BlogPost.png?raw=true "Medium Blog Post")

## Licensing, Authors, Acknowledgements<a name="licensing"></a>

Must give credit to StackOverflow for the collecting data and to Udacity because there are some pieces of code taken out from the DataScience Udacity Nanodegree classrooms. 
Otherwise, feel free to use the code here as you would like! 

## References <a name="references"></a>
 [Data Modeling with Postgres by Udacity](https://learn.udacity.com/nanodegrees/nd027/parts/cd0029/lessons/ls1961/concepts/1d3c5721-ca17-4483-a84a-a7e999b3d9a3) <br>
 [StackOverflow Raw Data](https://insights.stackoverflow.com/survey) <br>
 [Data Science Udacity Nanodegrees](https://www.udacity.com/school-of-data-science) <br>

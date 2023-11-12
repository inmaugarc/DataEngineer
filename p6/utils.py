
# Do all imports and installs here
import os
import configparser
import pandas as pd
import numpy as np
from datetime import datetime 
import missingno as msno
from pandas.io.json import json_normalize #package for flattening json in pandas df
from pandasql import sqldf

from pyspark.sql import SparkSession, SQLContext, GroupedData, HiveContext
import pyspark.sql.functions as funcs
from pyspark.sql.functions import isnan, when, count, col, sum, udf, dayofmonth, dayofweek, month, year, weekofyear, avg, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofweek, dayofmonth, weekofyear, date_format
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql import Row
import json 
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear


def to_parquet(df, folder, file):
    """
        INPUT:  dataframe,folder, table
        OUTPUT: table exported to a parquet file
    """
    
    file_path = folder + file
    
    print("... Exporting table {} to {}".format(file, file_path))
    
    df.write.mode("overwrite").parquet(file_path)
    
    print("... Table exported to a parquet file")
    
 
def check_numrows(df, expected_result):
    """
        INPUT:  dataframe, expected_result   
    """  
    num_rows = df.count()
    
    if (num_rows == expected_result):
         print("Data Quality Check for {} passed ok! ".format(df))
    else:
        print("Data Quality Check for {} not passed :( ".format(df))
        
    
    

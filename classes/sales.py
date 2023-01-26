
import logging
import json,os,re,sys,time
from typing import Callable,Optional 
# from pyspark.sql.dataframe import Dataframe 
from pyspark.sql import SparkSession 

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(project_dir)
LOG_FILE =  f"{project_dir}/logs- {os.path.basename(__file__)}.log"
print(LOG_FILE)
LOG_FORMAT = f"` "
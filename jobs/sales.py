

import logging
import json
import os
import re
import sys
from typing import Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# print(project_dir)
LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
# print(LOG_FILE)
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('py4j')

sys.path.insert(1, project_dir)
# print(sys.path)
from classes import class_pyspark

def main(project_dir: str) -> None:
    """
    STARTS a spark job
    """
    #class_pyspark.Sparkclass(config= {"myvar":"spark initiated"}).sparkStart()
    # print(project_dir)

    conf = openfile(f"{project_dir}/json/sales.json")
    spark = sparkStart(conf)


    '''
    Load some data to manipulate
    
    '''

    '''
    with pattern matching
    
    '''
    trasactionDf =importData(spark,f'{project_dir}/test_data/sales/transactions', ".json$")

    '''
    without pattern matching
    '''
    #trasactionDf =importData(spark,f'{project_dir}/test_data/sales/transactions')

    sparkStop(spark)


def openfile(filepath: str) -> dict:
    '''
    conditional : input variable should be STRING and the input filepath should exists
                    else no print --> no operation
    '''

    '''
    Dedicated file opener for JSON
    '''

    def openJSON(filepath: str) -> dict:
        if isinstance(filepath, str) and os.path.exists(filepath):
            with open(filepath, "r") as f:
                # print()
                data = json.load(f)
        return data
    return (openJSON(filepath))


def sparkStart(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        # print(conf)
        print("conf --> " , conf)
        return class_pyspark.Sparkclass(config={}).sparkStart(conf)


def sparkStop(spark: SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None


def importData(spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
    if isinstance(spark, SparkSession):
        return class_pyspark.Sparkclass(config={}).importData(spark,datapath, pattern)
    
        #print('spark ->',spark)


if __name__ == '__main__':
    main(project_dir)

import logging
import json,os,re,sys
from typing import Callable,Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(project_dir)
LOG_FILE =f"{project_dir}/logs- {os.path.basename(__file__)}.log"
print(LOG_FILE)
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level = logging.DEBUG , format = LOG_FORMAT)
logger = logging.getLogger('py4j')

sys.path.insert(1, project_dir)
# print(sys.path)
from classes import class_pyspark


def main():
    class_pyspark.Sparkclass(config= {"myvar":"spark initiated"}).sparkStart()

if __name__ == '__main__':
    main()



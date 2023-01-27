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


def main(project_dir:str) -> None:

    """
    STARTS a spark job
    """
    #class_pyspark.Sparkclass(config= {"myvar":"spark initiated"}).sparkStart()
    # print(project_dir)

    openfile(f"{project_dir}/json/sales.json")


def openfile(filepath: str) -> dict:
    '''

    conditional : input variable should be STRING and the input filepath should exists
                    else no print --> no operation

    '''
    print(filepath)

    def openJSON(filepath):
        isinstance(filepath,str) and os.path.exists(filepath):
        with open(filepath, r) as f:
            # print()
            data = json.load
        return data
    print(openJSON(filepath))




if __name__ == '__main__':
    main(project_dir)



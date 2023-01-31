#!/usr/bin/python

# tag::import[]

import json
import os
import re
import sys
import time
from typing import Any, Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext

# end::import[]


class Sparkclass:

    def __init__(self, config):
        self.config = config
        # self.config = config
        # self.myvar  = "hello spark initiated"

    def sparkStart(self, kwargs: dict):
        # print(kwargs)
        MASTER = kwargs['spark_conf']['master']
        APP_NAME = kwargs['spark_conf']['appname']
        LOG_LEVEL = kwargs['log']['level']

        def createSession(master: Optional[str] = "local[*]", app_name: Optional[str] = "myapp") -> SparkSession:
            '''
                Create a spark session
            '''

            spark = SparkSession.builder.appName(
                app_name).master(master).getOrCreate()
            return spark

        def getSettings(spark: SparkSession) -> None:
            print(spark)
            print(spark.SparkContext.getConf.getAll())

        def setLogging(spark: SparkSession, log_level: Optional[str] = None) -> None:
            spark.sparkContext.setLogLevel(log_level) if log_level else None

        def getSettings(spark: SparkSession) -> None:
            pass
        spark = createSession(MASTER, APP_NAME)
        getSettings(spark)

        return (spark)

    def importData(self,spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
        #print(spark, datapath, pattern)

            def fileOrDirectory(datapath:str) -> str:
                if isinstance(datapath,str) and os.path.exists(datapath):
                    if os.path.isdir(datapath):
                        return "dir"
                    elif os.path.isfile(datapath):
                        return "file"

            def openDirectory(datapath:str, pattern:Optional[str] = None) -> list:
                if isinstance(datapath, str) and os.path.exists(datapath):
                    Sparkclass(self.config).listDirectory(datapath)


            def openFile(datapath:str):
                if isinstance(datapath, str) and os.path.exists(datapath):
                    pass


            pathType =fileOrDirectory(datapath)
            openDirectory(datapath) if pathtype == "dir" else None


    def listDirectory(self,directory:str) -> list:
        def recursiveFilelist(directory):
            if os.path.exists(directory):
                filelist = []
                for dirpath, dirname,filename in os.walk(directory):
                    print(dirpath,dirname,filename)

        recursiveFilelist(directory)


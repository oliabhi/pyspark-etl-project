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

    def __init__(self, conf):
        self.conf = conf
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

        def getLoggng(spark: SparkSession):
            pass
        spark = createSession(MASTER, APP_NAME)
        getSettings(spark)

        return (spark)

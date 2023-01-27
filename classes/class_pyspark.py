#!/usr/bin/python

# tag::import[]

import json, os, re, sys, time
from typing import Any, Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

# end::import[]


class Sparkclass:

    def __init__(self, conf):
        self.conf = conf
        # self.config = config
        # self.myvar  = "hello spark initiated"

    def sparkStart(self, kwargs:dict):
        # print(kwargs)
        MASTER = kwargs['spark_conf']['master']
        APP_NAME = kwargs['spark_conf']['appname']
        LOG_LEVEL = kwargs['log']['level']

        def createSession(master , app_name):
            '''
                Create a spark session
            '''

            spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
            print(spark)
        createSession(MASTER,APP_NAME)

#!/usr/bin/python

# tag::import[]

import json, os, re, sys, time
from typing import Any, Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

# end::import[]


class Sparkclass:

    def __init__(self, config):
        self.config = config
        # self.config = config
        # self.myvar  = "hello spark initiated"

    def sparkStart(self):
        print(self.config['myvar'])

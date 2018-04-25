"""
This program is responsible for providing all the entry gates to spark.
Author - Puneet Tripathi
"""

import os
from pyspark.sql import SparkSession, SQLContext


class SparkApplication:
    """
    Class provides - spark, sc & sqlContext
    """
    def __init__(self):
        """
        Constructor to provide spark entry points
        """
        self.spark = self.get_spark()
        self.sc = self.spark.sparkContext
        self.sqlContext = self.get_sqlctx()

    def get_spark(self):
        """
        Function to provide spark session.
        :return: spark: SparkSession
        """
        # os.environ['HADOOP_HOME'] = 'C:\WinUtils'
        spark = SparkSession \
            .builder \
            .appName("SparkETL") \
            .getOrCreate()
        return spark

    def get_sqlctx(self):
        """
        Returns SQLContext
        :return: SQLContext
        """
        return SQLContext(self.sc)

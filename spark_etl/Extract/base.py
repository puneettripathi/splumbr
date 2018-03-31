"""
Class for all things Reader for ETL on Spark & Python
"""

import io
import zipfile
import os
from spark_etl.spark_session import SparkApplication


class Base(SparkApplication):
    """
    Class to provide all functionalities to Extract Data from various sources.
       -- CSVs & other delimited files
       -- Fixed Length Files
       -- Parquets
       -- Zips
       -- Avro
       -- Hive tables
       -- DBs
    Only Zips files are read and returned as RDD while all others return DataFrame
    """
    def __init__(self,
                 source=None,
                 source_path=None
                 ):
        """
        Cunstructor for Base Class

        :param source:
        :param source_path:
        """
        self.source = source
        self.source_path = source_path

    def read_delimited_files(self, delimiter, **kwargs):
        """
        Read Delimited Files as pyspark Dataframe

        :param delimiter: Delimiter for the file to read
        :param kwargs: Key word arguements
        :return: Spark DataFrame
        """
        df = self.spark.read.csv(self.source_path, sep=delimiter, **kwargs)
        return df

    def read_json_files(self, schema=None, **kwargs):
        """
        Reads JSON files to spark Dataframe

        :param schema: Optional Schema for the file to read
        :param kwargs: Key word arguements
        :return: Spark DataFrame
        """
        df = self.spark.read.json(self.source_path, schema=schema, **kwargs)
        return df

    def read_fix_length_files(self, record_splits, column_list):
        """
        This Function lets you work with Fixed length record files.

        :param record_splits: [[starting position, end postion), ....]
        :param column_list: List of columns for the Dataframe
        :return: Spark DataFrame
        """
        rdd = self.sc.textFile(self.source_path)
        split_rdd = rdd.map(lambda x: x[k[0]-1: k[1]] for k in record_splits)
        df = split_rdd.toDF(column_list)
        return df

    @staticmethod
    def zip_extract(zip_file):
        """

        :param zip_file:
        :return:
        """
        in_memory_data = io.BytesIO(zip_file[1])
        file_obj = zipfile.ZipFile(in_memory_data, "r")
        files = [i for i in file_obj.namelist()]
        return [(file, file_obj.open(file).read().decode()) for file in files]

    def read_zip_files(self, dlm):
        """
        Funtion reads zip file, even the ones containing mutiple Text files inside

        :param dlm:
        :return:
        """
        rdd = self.sc.binaryFiles(self.source_path)
        data_rdd = rdd.flatMap(
            lambda x: Base.zip_extract(x)).map(
            lambda x: x[1]).flatMap(
            lambda s: s.split("\n")).map(
            lambda x: x.split(dlm))
        return data_rdd

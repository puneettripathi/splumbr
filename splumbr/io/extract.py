"""
Class for all things Reader for ETL on Spark & Python
"""

import io
import zipfile
import os
from splumbr.spark_session import SparkApplication


class Extract(SparkApplication):
    """
    Class to provide all functionalities to io Data from various sources.
       -- CSVs & other delimited files
       -- Fixed Length Files
       -- Parquets
       -- Zips
       -- Avro
       -- Hive tables
       -- DBs
    Only Zips files are read and returned as RDD while all others return DataFrame
    Avro needs to use datadricks-avro jar
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
            lambda x: Extract.zip_extract(x)).map(
            lambda x: x[1]).flatMap(
            lambda s: s.split("\n")).map(
            lambda x: x.split(dlm))
        return data_rdd

    def read_parquet_files(self, **kwargs):
        """
        Reads Parquet files in a Dataframe

        :param kwargs:
        :return:
        """
        df = self.spark.read.parquet(self.source_path, **kwargs)
        return df

    def read_avro_files(self, **kwargs):
        """
        Funtion to read Avro files to Spark. Spark doesn't yet fully support Avro file format. It needs to use daatabricks-avro Jar.
        This is not a very neat implementation but it does solve the problem.

        :param kwargs:
        :return: dataframe
        """
        df = self.spark.read.format("com.databricks.spark.avro").load(self.source_path, **kwargs)
        return df

    def read_hive_table(self,
                        database,
                        table_name,
                        column_list=None,
                        partition_scan=None,
                        filter_conditions=None,
                        custom_sql=None):
        """
        Reads data from Hive table and load the data to a Spark dataframe.

        :param database: Name of the Hive Database
        :param table_name: Name of the table to fetch rows from
        :param column_list: Default "*", else the columns to select from table
        :param partition_scan: Partition Scan allows you to specify a list of (k,v) pair --> [(partition_column, partition_value)]
        :param filter_conditions: Filter condition to select data from table, must be string like "class = 5 and marks >= 70"
        :return: df - Spark Dataframe
        """
        if column_list is None or len(column_list) == 0:
            sql_stmt = "select * from " + database + "." + table_name + " where 1=1"
        else:
            sql_stmt = "select " + ", ".join(column_list) + " from " + database + "." + table_name + " where 1=1"

        if filter_conditions is not None:
            sql_stmt = sql_stmt + " and " + filter_conditions

        if partition_scan is not None and len(partition_scan) > 0:
            partition_stmt = "and " + " and ".join([i[0] + "=" + str(i[1])
                                                    if type(i[1]) in (str, int, float)
                                                    else i[0] + " in " + "(" + ", ".join(map(str, i[1])) + ")"
                                                    for i in partition_scan])

            sql_stmt = sql_stmt + partition_stmt

        if custom_sql is not None:
            df = self.spark.sql(custom_sql)
        else:
            df = self.spark.sql(sql_stmt)

        return df

    def read_db_table(self,
                      db_type,
                      ):
        """
        This function reads data from RDBMS table and returns a Spark dataframe
         ---- Not implemented yet ----

        :return:
        """
        raise NotImplementedError

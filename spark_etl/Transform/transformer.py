"""
Base Package for applying tranformations to the dataframe.
"""

from spark_etl import logger
from spark_etl.spark_session import SparkApplication
from pyspark.sql.functions import lit, udf, col


class Transformer(SparkApplication):
    """
    Applies basic transformations on input dataframe. Basic tranformations involve
      - derived columns
      - datatype change
      - drop/keep (multiple) columns
      - checkpointing dataframe to break lineage
      - treat outliers
      - handle missing values
      - missing value imputations

    """
    def __init__(self,
                 source_data):
        """
        Tranfformer Constructor takes dataframe as input and applies transformations to it.

        :param source_data:
        """
        self.df = source_data

    def add_derived_column(self,
                           col_name,
                           expression=None):
        """
        Adds new Column to df

        :param col_name:
        :param expression:
        :return:
        """
        if expression is None:
            expression = lit(None)
        elif type(expression) == int:
            expression = lit(expression)
        else:
            expression = expression
        self.df = self.df.withColumn(col_name,expression)

    def check_point(self):
        """
        This method is to break lineage of transformations. This method save the resulting dataFrame in disk
        cutting the lineage.
        """
        logger.info("checkpointing dataframe to break lineage.")
        self.df.checkpoint()
        self.df.count()
        self.df = self.spark.createDataFrame(self.df, self.df.schema)

    def drop_columns(self, column_list=None):
        """
        Drops the list of columns provided
        :param column_list:
        :return:
        """
        assert isinstance(column_list, (str,list)), "Error: column_list must be either list or string"
        if isinstance(column_list, list):
            self.df.drop(*column_list)
        else:
            self.df.drop(column_list)

    def keep_columns(self, column_list=None):
        """
        Drops the list of columns provided
        :param column_list:
        :return:
        """
        assert isinstance(column_list, list), "ERROR: arguement column_list must be a list."
        if isinstance(column_list, list):
            self.df.select(*column_list)

    def change_column_datatype(self,
                               column_name=None,
                               new_data_type=None,
                               column_dtype_dict=None):
        """
        Changes data type of column.

        :param column_name:
        :param new_data_type:
        :param column_dtype_dict:
        :return:
        """
        old_columns = self.df.columns
        if column_dtype_dict is not None:
            _columns = [colm for colm in old_columns if colm not in column_dtype_dict.keys()]
            new_columns = ["cast(" + i[0] + ' as ' + i[1] + ") as " + i[0] for i in column_dtype_dict.items()]
            self.df = self.df.selectExpr(*_columns,
                                         *new_columns)
        elif column_name is not None and new_data_type is not None:
            _columns = [colm for colm in old_columns if colm != column_name]
            self.df = self.df.selectExpr(*_columns,
                                         "cast(" + column_name + " as " + new_data_type)
        else:
            logger.error("change_column_datatype must take either column_dtype_dict or both column_name and new_data_type")

    def show(self, n=20):
        """
        Shows 20 rows from dataframe.
        """
        self.df.show(n=n,
                     truncate=True)

    @property
    def df(self):
        """
        The dataframe being transformed(as is).

        :return: df
        """
        return self.df

    def apply_udf_to_str_cols(self, column_list, func, returnType):
        """
        Applies the User Defined Funtion to the string type columns in the Input dataframe.

        :param column_list: List of columns to tranform
        :param func: UserDefinedFunction to apply to columnns
        :param returnType: Return type of UDF
        ## This function is not well tested.
        """
        # https://stackoverflow.com/questions/34037889/apply-same-function-to-all-fields-of-spark-dataframe-row
        assert set(column_list).issubset(self.df.columns), "ERROR: Column List provided is not in DataFrame columns."
        columns_to_use = [x[0] for x in self.df.dtypes if x[1] == 'string']
        assert set(column_list).issubset(set(columns_to_use)), "ERROR: Columns in column_list are not string."

        fx = udf(func, returnType)
        self.df = self.df.select(*[fx(col(c)).alias(c) for c in self.df.columns])

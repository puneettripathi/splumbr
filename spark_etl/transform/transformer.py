"""
Base Package for applying tranformations to the dataframe.
"""

from spark_etl import logger
from spark_etl.spark_session import SparkApplication
from pyspark.sql.functions import lit, udf, col
from functools import reduce


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

    def __init__(self, source_data):
        """
        Transformer Constructor takes dataframe as input and applies transformations to it.

        :param source_data:
        """
        super().__init__()
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
        self.df = self.df.withColumn(col_name, expression)

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
        if isinstance(column_list, str):
            column_list = column_list.split(",")

        assert isinstance(column_list, (str, list)), "Error: column_list must be either list or string"
        self.df.drop(*column_list)

    def keep_columns(self, column_list=None):
        """
        Keeps only the list of columns provided

        :param column_list:
        :return:
        """
        if isinstance(column_list, str):
            column_list = column_list.split(",")

        assert isinstance(column_list, list), "ERROR: arguement column_list must be a list."
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
            logger.error(
                "change_column_datatype must take either column_dtype_dict or both column_name and new_data_type")

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

    def detect_outlier_cutoffs(self, column, quantiles_points=(0.20, 0.80), range_cutoff=1.5):
        """

        :param column:
        :param quantiles_points:
        :param range_cutoff:
        :return:
        """
        quantiles = self.df.approxQuantiles(column,
                                            quantiles_points,
                                            0.0)
        iqr = quantiles[1] - quantiles[0]
        return quantiles[0] - range_cutoff * iqr, quantiles[1] + range_cutoff * iqr

    def remove_outlier(self, column, **kwargs):
        """

        :param column:
        :param kwargs:
        :return:
        """
        cutoff_outlier = self.detect_outlier_cutoffs(column, **kwargs)
        self.df.filter(column + " < " + str(cutoff_outlier[0]) + " or " + column + " > " + str(cutoff_outlier[1]))

    def handle_missing_values(self, column_list, strategy=None, value=None):
        """
        Imputes the missing values in a dataframe in column list provided by the user.

        :param column_list: list of columns to be imputed
        :param strategy: mean, median, mode - for only numeric columns
        :param value: sets a constant value for all variables in column list - works for both String and Numeric columns.
        """
        col_fill = None
        if isinstance(column_list, str):
            column_list = [column_list]
        else:
            column_list = column_list
        assert isinstance(column_list, list), "ERROR: column must be a list or string"

        if value is not None:
            logger.warn("value is provided ignoring strategy.")
            col_fill = {column: value for column in column_list}
        elif strategy.lower() in ('mean', 'median', 'mode'):
            assert len(column_list) == len(
                [tpl
                 for tpl in self.df.select(*column_list).dtypes
                 if tpl[1] in ('int', 'bigint', 'double', 'long')
                 ]),\
                "ERROR: column_list contains invalid datatype valid datatypes are int, bigint, double, long"
            if strategy.lower() == 'medeian':
                col_fill = {column: self.df.approxQuantile(column, [0.5], 0.25) for column in column_list}
            elif strategy.lower() == 'mean':
                _mean_dict = {column: 'avg' for column in column_list}
                _mean_values = self.df.agg(_mean_dict).collect()[0].asDict()
                col_fill = {k[4:-1]: v for k, v in _mean_values.iteritems()}
            else:
                mode = reduce(
                    lambda a, b: a.join(b, "id"),
                    [
                        self.df.groupBy(column).count().sort(col("count").desc()).limit(1).select(
                            lit(1).alias("id"),
                            col(column).alias(column)
                        )
                        for column in column_list
                    ]
                )
                col_fill = mode.drop("id").collect()[0].asDict()
        else:
            logger.error("either provide value or a strategy for filling missing values")

        if col_fill is not None:
            self.df = self.df.na.fill(col_fill)

    @df.setter
    def df(self, value):
        self.df = value

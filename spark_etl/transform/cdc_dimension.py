"""
Change Data Capture ~ SCD Type 1 ~ UpSert
"""
from spark_etl import logger
from spark_etl.spark_session import SparkApplication
from pyspark.sql.functions import concat


class CDCDimension(SparkApplication):
    """
    CDC Dimension captures any change that occured in Dimensions over a period of time. It finds records
       - with no change
       - which updated
       - new records

    then it appends the records to create new dataset. As of now deletion of older dimension is not handled.

    """

    def __init__(self,
                 source_df=None,
                 change_df=None,
                 source_location=None,
                 change_data_location=None,
                 key=None
                 ):
        if source_df is not None:
            self.source_df = source_df
        elif source_location is not None:
            self.source_df = self._get_data(source_location)
        else:
            logger.error("Can't read source df, source_df and source_location both are missing.")

        if change_df is not None:
            self.change_df = change_df
        elif change_data_location is not None:
            self.change_df = self._get_data(change_data_location)
        else:
            logger.error("Can't read source df, source_df and source_location both are missing.")

        if type(key) == str:
            self.key = key
        elif type(key) in (list, tuple, set):
            self._create_composite_key(key)
            self.key = "key_column"

        self.data = self.change_captured()

    def _get_data(self, src_data):
        """
        Reads data from parquet files to Spark dataframe.

        :param src_data:
        :return: pyspark.sql.dataframe.DataFrame
        """
        return self.spark.read.parquet(src_data)

    @property
    def data(self):
        """
        This function return the dataframe of the class

        :return: pyspark.sql.dataframe.DataFrame
        """
        return self.data

    def _create_composite_key(self, key_list):
        """
        """
        self.source_df.withColumn("key_column", concat(*key_list))
        self.change_df.withColumn("key_column", concat(*key_list))

    def _no_change(self):
        """
        Function capture rows that didn't change in source and change dataframes.

        :return: pyspark.sql.dataframe.DataFrame
        """
        if self.key in self.source_df.columns and self.key in self.change_df.columns:
            _no_change = self.source_df.alias("a"
                                              ).join(self.change_df.alias("b"),
                                                     self.change_df[self.key] == self.source_df[self.key],
                                                     "left"
                                                     ).where(self.change_df[self.key].isNull()
                                                             ).selectExpr("a.*")
        else:
            logger.error("change capture key is not in the source or change df")
        return _no_change

    def _new_records(self):
        """
        Create rows that are new records and must be inserted for CDC.

        :return: pyspark.sql.dataframe.DataFrame
        """
        if self.key in self.source_df.columns and self.key in self.change_df.columns:
            _insert = self.change_df.alias("a"
                                           ).join(self.source_df.alias("b"),
                                                  self.change_df[self.key] == self.source_df[self.key],
                                                  "left"
                                                  ).where(self.source_df[self.key].isNull()
                                                          ).selectExpr("x.*")
        else:
            logger.error("change capture key is not in the source or change df")
        return _insert

    def _update_records(self):
        """
        Creates dataframe that captures the updated records.

        :return: pyspark.sql.dataframe.DataFrame
        """
        if self.key in self.source_df.columns and self.key in self.change_df.columns:
            _update = self.change_df.alias("a"
                                           ).join(self.source_df.alias("b"),
                                                  self.change_df[self.key] == self.source_df[self.key]
                                                  ).selectExpr("x.*")
        else:
            logger.error("change capture key is not in the source or change df")
        return _update

    def change_captured(self):
        """
        Captures the change in new dimension dataframe and old dimension dataframe

        :return: pyspark.sql.dataframe.DataFrame
        """
        update_recs = self._update_records()
        new_recs = self._new_records()
        no_change_recs = self._no_change()
        return no_change_recs.unionAll(new_recs).unionAll(update_recs)

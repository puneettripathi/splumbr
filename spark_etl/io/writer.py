"""
Class for all things Loader for ETL on Spark & Python
"""

from spark_etl import logger
from spark_etl.spark_session import SparkApplication
from pyspark.sql.functions import lit


class Writer(SparkApplication):
    """
    Class to provide all functionalities to io Data from various sources.
           -- Delimited files
           -- JSON
           -- Parquets
           -- ORC
           -- Avro - Needs databricks-avro jar to load data to avro format.
           -- Hive tables
           -- DBs - Not Supported yet
    Only Zips files are read and returned as RDD while all others return DataFrame
    Avro needs to use datadricks-avro jar
    """

    def __init__(self,
                 target=None,
                 target_path=None,
                 write_mode="overwrite"):
        """
        Class Writer provides utilities to write data to disk.
        Default write_mode is "overwrite", update as needed.

        :param target:
        :param target_path:
        :param write_mode:
        """
        super().__init__()
        self.target = target
        self.target_path = target_path
        self.write_mode = write_mode

    def write_to_delimited(self,
                           df,
                           delimiter="|",
                           **kwargs):
        """
        Writes dataframe as CSV to Writer's target_path.

        :param df: Dataframe to write
        :param delimiter: delimiter to use for writing
        :param kwargs: keyword args
        """
        df.write.mode(self.write_mode
                      ).csv(self.target_path,
                            sep=delimiter,
                            **kwargs)
        logger.info("Write Complete.")

    def write_to_json(self,
                      df,
                      **kwargs):
        """
        Writes dataframe as JSON to Writer's target_path.

        :param df: Dataframe to write
        :param kwargs: keyword args
        """
        df.write.mode(self.write_mode
                      ).json(self.target_path,
                             **kwargs)
        logger.info("Write Complete.")

    def write_to_orc(self,
                     df,
                     **kwargs):
        """
        Writes dataframe as ORC to Writer's target_path.

        :param df: Dataframe to write
        :param kwargs: keyword args
        """
        df.write.mode(self.write_mode
                      ).orc(self.target_path,
                            **kwargs)
        logger.info("Write Complete.")

    def write_to_parquet(self,
                         df,
                         **kwargs):
        """
        Writes dataframe as ORC to Writer's target_path.

        :param df: Dataframe to write
        :param kwargs: keyword args
        """
        df.write.mode(self.write_mode
                      ).parquet(self.target_path,
                                **kwargs)
        logger.info("Write Complete.")

    def write_to_avro(self,
                      df,
                      **kwargs):
        """
        Writes dataframe as AVRO to Writer's target_path.
        Requires databricks-avro jar to write dataframe to avro.

        :param df: Dataframe to write
        :param kwargs: keyword args
        """
        df.write.mode(self.write_mode
                      ).format("com.databricks.spark.avro"
                               ).save(self.target_path,
                                      **kwargs)
        logger.info("Write Complete.")

    def write_to_hive_table(self,
                            df,
                            db_name,
                            table_name,
                            handle_schema=False
                            ):
        """
        Writes dataframe to Hive table.
        Default mode is overwrite which doesn't care for schema.

        :param df: Dataframe to write
        :param db_name: Hive Database
        :param table_name: Hive Table name
        :param ignore_schema: Default False,
               if True it rebalances the columns in dataframe according to hivetable
        """
        table_name = db_name + "." + table_name
        hive_df = self.sqlContext(table_name)
        if self.write_mode == "overwrite":
            df.write.insertInto(table_name,
                                overwrite=True)
        else:
            if handle_schema:
                df_cols = set(df.schema)
                master_cols = set(hive_df.schema)
                common_cols = [field.name for field in df_cols if field in master_cols]
                col_hive_notdf = [(i.name, i.dataType) for i in master_cols - df_cols]
                if len(col_hive_notdf) > 0:
                    select_expr = [lit(None).cast(field[1]).alias(field[0]) for field in col_hive_notdf]
                    df.select(*common_cols, *select_expr)
                else:
                    df.select(*common_cols)

            else:
                df.write.insertInto(table_name,
                                    overwrite=True)
        logger.info("Write Complete.")

    def write_to_db(self):
        raise NotImplementedError

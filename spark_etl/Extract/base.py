import pyspark
from spark_etl.spark_session import SparkApplication

# 13470639


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
    """
    def __init__(self,
                 source=None,
                 source_path=None,

                 ):
        self.source = source
        self.source_path = source_path

    def read_text_files(self):
        """

        :return:
        """

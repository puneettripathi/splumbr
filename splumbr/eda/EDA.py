from pyspark.sql.functions import *
import matplotlib
from pyspark.ml.feature import StringIndexer
from splumbr.io import extract

class EDA:
    """
    Exploratory Data Analysis.
    """
    def __init__(self,
                 df=None,
                 source=None,
                 source_path=None):
        """
        EDA Constructor. This is place where all data exploration will happen.

        :param df:
        :param source:
        :param source_path:
        """
        if df is None:
            if source is not None and source_path is not None:
                self.df = extract.Extract(source, source_path).data
            else:
                print("Parameter issue.")
                print("Either df or source & source_path needs to be provided. All can't be None.")
        else:
            self.df = df

    def summarize(self,
                  grouping_key=[],
                  metrics={}):
        """

        :param grouping_key:
        :param metrics:
        :return:
        """
        if type(grouping_key) == str:
            grouped_df = self.df.groupBy(grouping_key)
        elif type(grouping_key) == list:
            grouped_df = self.df.groupBy(*grouping_key)
        else:
            print("Grouping Key should eiter be a string(for single column) or a list(as many columns)")
            exit(1)

        if type(metrics) == dict:
            ret_df = grouped_df.agg({col: metrics[col] for col in metrics})
        else:
            print("Metrics must always be a dictionary - {column: func_to_applied_on_column, ....} ")
            exit(1)

        return ret_df

    def explore(self,
                calculation_map):
        """
        THis function produces following charts for EDA -
            - Categorical - Bar Plots/Treemaps
            - Numeric - Violin/boxplot & Histogram/Density
            - Date - line chart

        :return:
        """
        _categorical_columns = calculation_map['categorical']
        _numeric_columns = calculation_map['numeric']
        _character_columns = calculation_map['character']
        _date_columns = calculation_map['date']

        metrics_cat = {var: self.df.groupBy(var).count() for var in _categorical_columns}


    def generate_plot(self):
        pass

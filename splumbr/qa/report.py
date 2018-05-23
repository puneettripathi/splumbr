"""
Prepares a report on the input dataframe.
"""

class Report:
    """
    Class to prepare a comprehensive reports on the input dataframe using stat parameters.
    """
    def __init__(self,
                 df,
                 column_list=None):
        """

        :param df:
        :param column_list:
        """
        self.df = df
        self.column_list = column_list
        self.type = {
            "num": ["int", "bigint", "long", "double"],
            "string": ["str"]
        }

    def set_col_data_type(self, type="num"):
        """

        :param type:
        :return:
        """
        if self.column_list is not None:
            dtypes = self.df.select(*self.column_list).dtypes
        else:
            dtypes = self.df.dtypes
            self.column_list = self.df.columns

        return [colm for colm in self.column_list
                if colm in filter(lambda x: x[0] in self.type.get(type), dtypes)]

    def prepare_stat_params(self):
        """

        :return:
        """
        dtypes_renormed_num = self.set_col_data_type()
        dtypes_renormed_str = self.set_col_data_type("str")
        mean_cols = {colm: "count" for colm in dtypes_renormed_num}
        return {"str_cols": dtypes_renormed_str,
                "mean_cols_stats": mean_cols}

    def prepare_stats(self):
        """

        :return: stats: dict
        """
        _stat_params = self.prepare_stat_params()
        _row_count_df = self.df.count()
        _cols_df = self.df.columns
        _mean_stat = self.df.agg(_stat_params["mean_cols_stats"]).collect()[0].asDict()
        _str_stats = [{colm: self.df.groupby(colm).count().collect()} for colm in _stat_params["str_cols"]]
        stats = _mean_stat
        stats["row_count"] = _row_count_df
        stats["coluns"] = _cols_df
        stats["stats_str_cols"] = _str_stats
        return stats

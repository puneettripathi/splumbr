"""
Basic Sanity Check Framework for two DataFrames
"""

class BasicSanityCheck:
    """

    """
    def __init__(self,
                 source_df,
                 compare_df,
                 qa_config=None):
        """

        :param source_df:
        :param compare_df:
        :param qa_config: Sample qa_Config
        {"checks": [
            {
                "type": "absolute",
                "column": "salary",
                "metric_expr": "avg",
                "compare_expr": "avg,
                "threshold": []
            },
            {
                "type": "range",
                "column": "balance",
                "metric_expr": "avg",
                "compare_expr": "avg",
                "threshold": [-50,50]
            }
        ]}
        """
        self.source_df = source_df
        self.compare_df = compare_df
        self.qa_battery = BasicSanityCheck.get_checks(qa_config)

    @staticmethod
    def get_checks(qa_config):
        """

        :param qa_config:
        :return: list of checks to be performed
        """
        if qa_config is not None:
            return qa_config.get("checks")
        else:
            return []

    def apply_battery(self):
        """

        :return: list
        """
        result_set = [
            self.source_df.agg({check.get("column"): check.get("metric_expr")}).collect()[0].asDict()
            for check in self.qa_battery
        ]
        compare_set = [
            self.compare_df.agg({check.get("column"): check.get("compare_expr")}).collect()[0].asDict()
            for check in self.qa_battery
        ]
        return [
            result_set[itr] == compare_set[itr]
            if self.qa_battery[itr].get("type").lower() == "absolute"
            else compare_set[itr] + compare_set[itr]*self.qa_battery[itr].get("threshold")[1] <= result_set[itr] <= compare_set[itr] + compare_set[itr]*self.qa_battery[itr].get("threshold")[1]
            if self.qa_battery[itr].get("type").lower() == "absolute"
            else None
            for itr in range(len(self.qa_battery))
        ]

    def verify_results(self):
        """

        :return: list
        """
        battery_result = self.apply_battery()
        return ["Pass" if val == True
                else "Fail"
                for val in battery_result]

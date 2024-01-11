import pandas as pd


class StatisticsDataframeFormatter:

    @staticmethod
    def select_and_sort_values(self, df: pd.DataFrame, col_statistics: str):
        """
        Formats the Shift data portal data by selecting and sorting the columns
        """
        list_cols_sort = ["group_type", "group_name", "year"]
        list_other_cols = [col for col in df.columns if col not in list_cols_sort + [col_statistics]]
        list_cols_sort = list_cols_sort + list_other_cols
        list_cols_select = list_cols_sort + [col_statistics]
        return df.sort_values(list_cols_sort)[list_cols_select]
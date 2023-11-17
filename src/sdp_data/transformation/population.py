import pandas as pd


class GapMinderCleaner:

    def __init__(self):
        self.equivalence_dict = {'k': 1e3, 'M': 1e6, 'B': 1e9}
        self.max_year = 2021

    def dirty_string_to_int(self, dirty_string: str):
        """
        Cleans values such as 3.35M into 3350000.
        :param dirty_string: (str) the string to convert in integer
        :return:
        """
        for key in self.equivalence_dict.keys():
            if key in dirty_string:
                dirty_string = dirty_string.replace(key, '')
                units = float(dirty_string) * self.equivalence_dict[key]
                return int(units)

    @staticmethod
    def unstack_dataframe_to_serie(df: pd.DataFrame):
        df = df.unstack().reset_index()
        df.columns = ["year", "country", "population"]
        return df

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """

        :return:
        """
        # clean the numbers
        df = df.applymap(lambda element: self.dirty_string_to_int(element))

        # unstack to a unique pandas serie
        df = self.unstack_dataframe_to_serie(df)
        df = df[df["year"] <= self.max_year]

        # TODO - ajouter la conversion en anglais ?
        return df


class PopulationCleaner:

    def __init__(self):
        self.max_year = 2020

    @staticmethod
    def unstack_dataframe_to_serie(df: pd.Da):
        df = df.unstack().reset_index()
        df.columns = ["year", "country", "population"]
        return df

    def run(self, df_population: pd.DataFrame) -> pd.DataFrame:

        # only keep useful columns
        df_population = df_population.set_index("Country Name")
        df_population = df_population.drop(["Country Code", "Indicator Name", "Indicator Code", "col_65"], axis=1)

        # unstack to a unique pandas serie
        df_population = self.unstack_dataframe_to_serie(df_population)
        df_population = df_population[df_population["year"] <= self.max_year]

        # TODO - ajouter la conversion en anglais ?
        return df_population

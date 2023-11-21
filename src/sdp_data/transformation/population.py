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
    def unstack_dataframe_to_serie(df: pd.DataFrame):
        df = df.unstack().reset_index()
        df.columns = ["year", "country", "population"]
        return df

    @staticmethod
    def convert_countries_from_french_to_english(df_population):
        return df_population  # TODO - remettre en place la fonction ? Dans Dataiku, cela était utilisé dans une custom function Dataiku.

    def run(self, df_population: pd.DataFrame, df_countries_and_zones: pd.DataFrame) -> pd.DataFrame:
        """
        Computes the total population for each year, each country and each geographic zone.
        :param df_population: (dataframe) where each row is a country, each column a year and each value, the population for this year and country.
        :param df_countries_and_zones: (dataframe) listing all countries through columns "group_type", "group_name" and "country"
        :return:
        """
        # only keep useful columns for population
        df_population = df_population.set_index("Country Name")
        df_population = df_population.drop(["Country Code", "Indicator Name", "Indicator Code", "col_65"], axis=1)

        # unstack to a unique pandas serie
        df_population = self.unstack_dataframe_to_serie(df_population)
        df_population = df_population[df_population["year"] <= self.max_year]

        # TODO - ajouter la conversion en anglais ?
        return df_population

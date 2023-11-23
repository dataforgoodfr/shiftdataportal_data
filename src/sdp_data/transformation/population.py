import pandas as pd

# TODO - à revoir
"""
-> traduction des noms de pays FR -> EN avec un autre package.
-> Revue des valeurs manquantes "zone supprimées".
"""


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

    def __init__(self, country_translations):
        self.max_year = 2020
        self.country_translations = country_translations

    @staticmethod
    def unstack_dataframe_to_serie(df: pd.DataFrame):
        df = df.unstack().reset_index()
        df.columns = ["year", "country", "population"]
        return df

    def convert_countries_from_french_to_english(self, df_population):
        df_population["country"] = df_population["country"].replace(self.country_translations)
        return df_population

    def run(self, df_population: pd.DataFrame, df_countries_and_zones: pd.DataFrame) -> pd.DataFrame:
        """
        Computes the total population for each year, each country and each geographic zone.
        :param df_population: (dataframe) where each row is a country, each column a year and each value, the population for this year and country.
        :param df_countries_and_zones: (dataframe) listing all countries through columns "group_type", "group_name" and "country"
        :return:
        """
        # only keep useful columns for population and unstack to a unique pandas serie
        df_population = df_population.set_index("Country Name")
        df_population = df_population.drop(["Country Code", "Indicator Name", "Indicator Code", "col_65"], axis=1)
        df_population = self.unstack_dataframe_to_serie(df_population)
        df_population = df_population.dropna()

        # filter time
        df_population["year"] = pd.to_numeric(df_population["year"])
        df_population = df_population[df_population["year"] < self.max_year]

        # convert countries from french to english
        df_population = self.convert_countries_from_french_to_english(df_population)

        # compute total population per zone
        df_total_population_per_zone = df_countries_and_zones.merge(df_population, how="left", left_on="country", right_on="country")
        df_total_population_per_zone = df_total_population_per_zone.groupby(["group_type", "group_name", "year"])["population"].sum()
        df_total_population_per_zone = df_total_population_per_zone.reset_index()

        # compute total population per country
        df_total_population_per_country = df_population.rename({"country": "group_name"}, axis=1)
        df_total_population_per_country["group_type"] = "country"

        # concatenate countries and zones populations
        df_population_per_zone_and_countries = pd.concat([df_total_population_per_zone, df_total_population_per_country], axis=0)

        return df_population_per_zone_and_countries

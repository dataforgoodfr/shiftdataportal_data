import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish
# TODO - à revoir
"""
-> Revue des valeurs manquantes "zone supprimées" pour PopulationCleaner.
-> ajouter des tests unitaires.
-> mutuliser le code entre GapMinderCleaner et 
"""


class GapMinderPerZoneAndCountryProcessor:

    def __init__(self):
        self.equivalence_dict = {'k': 1e3, 'M': 1e6, 'B': 1e9}
        self.max_year = 2021

    def dirty_string_to_int(self, dirty_string: str):
        """
        Cleans values such as 3.35M into 3350000.
        :param dirty_string: (str) the string to convert in integer
        :return:
        """
        dirty_string = str(dirty_string)
        for key in self.equivalence_dict.keys():
            if key in dirty_string:
                dirty_string = dirty_string.replace(key, '')
                units = float(dirty_string) * self.equivalence_dict[key]
                return int(units)

        return int(dirty_string)

    @staticmethod
    def unstack_dataframe_to_serie(df: pd.DataFrame):
        df = df.unstack().reset_index()
        df.columns = ["year", "country", "population"]
        return df

    def run(self, df_gapminder: pd.DataFrame, df_country: pd.DataFrame) -> pd.DataFrame:
        """
        Computes the total gapminder for each year, each country and each geographic zone.
        :return:
        """
        # clean the numbers
        print("\n----- compute GapMinder for each country and each zone")
        df_gapminder = df_gapminder.set_index("country")
        df_gapminder = df_gapminder.applymap(lambda element: self.dirty_string_to_int(element))

        # unstack to a unique pandas serie and filter time
        df_gapminder = self.unstack_dataframe_to_serie(df_gapminder)
        df_gapminder["year"] = pd.to_numeric(df_gapminder["year"])
        df_gapminder = df_gapminder[(df_gapminder["year"].astype(int) <= int(self.max_year)) & (df_gapminder['year'].notnull())]


        # convert countries from french to english
        df_gapminder["country"] = CountryTranslatorFrenchToEnglish().run(df_gapminder["country"], raise_errors=False)

        # join with countries
        df_total_gapminder_per_zone = (pd.merge(df_country, df_gapminder, how='left', left_on='country', right_on='country')
                                       .groupby(['group_type', 'group_name', 'year'])
                                       .agg({'population': 'sum'})
                                       .reset_index()
                                       )

        # compute total gapminder per country
        df_total_gapminder_per_country = df_gapminder.copy()
        df_total_gapminder_per_country = df_total_gapminder_per_country.rename({"country": "group_name"}, axis=1)
        df_total_gapminder_per_country["group_type"] = "country"
        df_total_gapminder_per_country = df_total_gapminder_per_country[["group_type", "group_name", "year", "population"]]

        # concatenate countries and zones populations
        df_gapminder_per_zone_and_countries = pd.concat([df_total_gapminder_per_zone, df_total_gapminder_per_country], axis=0)

        return df_gapminder_per_zone_and_countries


class PopulationPerZoneAndCountryProcessor:

    def __init__(self):
        self.max_year = 2020

    @staticmethod
    def unstack_dataframe_to_serie(df: pd.DataFrame):
        df = df.unstack().reset_index()
        df.columns = ["year", "country", "population"]
        return df

    def run(self, df_population: pd.DataFrame, df_countries_and_zones: pd.DataFrame) -> pd.DataFrame:
        """
        Computes the total population for each year, each country and each geographic zone.
        :param df_population: (dataframe) where each row is a country, each column a year and each value, the population for this year and country.
        :param df_countries_and_zones: (dataframe) listing all countries through columns "group_type", "group_name" and "country"
        :return:
        """
        # only keep useful columns for population and unstack to a unique pandas serie
        print("\n----- compute Population for each country and each zone")
        df_population = df_population.set_index("Country Name")
        df_population = df_population.drop(["Country Code", "Indicator Name", "Indicator Code", "col_65"], axis=1)
        df_population = self.unstack_dataframe_to_serie(df_population)
        df_population = df_population.dropna()

        # filter time
        df_population["year"] = pd.to_numeric(df_population["year"])
        df_population = df_population[df_population["year"] < self.max_year]

        # convert countries from french to english
        df_population["country"] = CountryTranslatorFrenchToEnglish().run(df_population["country"], raise_errors=False)

        # compute total population per zone
        df_total_population_per_zone = (
            pd.merge(df_countries_and_zones, df_population, how='left', left_on='country', right_on='country')
            .groupby(['group_type', 'group_name', 'year'])
            .agg({'population': 'sum'})
            .reset_index()
            )

        # compute total population per country
        df_total_population_per_country = df_population.rename({"country": "group_name"}, axis=1)
        df_total_population_per_country["group_type"] = "country"

        # concatenate countries and zones populations
        df_population_per_zone_and_countries = pd.concat([df_total_population_per_zone, df_total_population_per_country], axis=0)

        return df_population_per_zone_and_countries


class StatisticsPerCapitaJoiner:

    @staticmethod
    def join_inner(df_statistics: pd.DataFrame, df_population_per_zone_and_countries: pd.DataFrame):
        """
        Computes statistics per capita by joinining on the population dataframe
        :param df_statistics: (dataframe) containing statistics and columns ["group_type", "group_name", "year"]
        :param df_population_per_zone_and_countries: (dataframe) containing population and columns ["group_type", "group_name", "year"]
        :return:
        """
        df_stats_per_capita = df_statistics.merge(df_population_per_zone_and_countries, how="inner",
                                                                   left_on=["group_type", "group_name", "year"],
                                                                   right_on=["group_type", "group_name", "year"])
        return df_stats_per_capita

    def run_historical_emissions_per_capita(self, df_historical_co2, df_population):
        df_stats_per_capita = self.join_inner(df_historical_co2, df_population)
        df_stats_per_capita["co2_per_capita"] = df_stats_per_capita["co2"] / df_stats_per_capita["population"]
        return df_stats_per_capita

    def run_eora_cba_per_capita(self, df_footprint_vs_territorial, df_population):
        df_stats_per_capita = self.join_inner(df_footprint_vs_territorial, df_population)
        df_stats_per_capita["co2_per_capita"] = df_stats_per_capita["co2"] / df_stats_per_capita["population"]
        df_stats_per_capita["co2_per_capita_unit"] = "MtCO2 per capita"
        return df_stats_per_capita

    def run_ghg_per_capita(self, df_ghg_by_sector, df_population):
        df_ghg_by_sector = df_ghg_by_sector.groupby(["source", "group_type", "group_name", "year"]).agg({'co2': "sum", "ghg_unit": "first"})
        df_ghg_per_capita = self.join_inner(df_ghg_by_sector, df_population)
        df_ghg_per_capita["ghg_per_capita"] = df_ghg_per_capita["ghg"] / df_ghg_per_capita["population"]
        return df_ghg_per_capita

    def run_final_energy_consumption_per_capita(self, df_final_energy_consumption, df_population):
        df_statistics_per_capita = self.join_inner(df_final_energy_consumption, df_population)
        df_statistics_per_capita["final_energy_per_capita"] = df_statistics_per_capita["final_energy"] / df_statistics_per_capita["population"]
        return df_statistics_per_capita

    def run_energy_per_capita(self, df_final_energy_consumption, df_population):
        df_statistics_per_capita = self.join_inner(df_final_energy_consumption, df_population)
        df_statistics_per_capita["energy_per_capita"] = df_statistics_per_capita["energy"] / df_statistics_per_capita["population"]
        return df_statistics_per_capita



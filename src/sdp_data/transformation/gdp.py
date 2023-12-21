import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish
from sdp_data.utils.iso3166 import countries_by_alpha3


class GdpMaddissonPerZoneAndCountryProcessor:

    def __init__(self, year_min=1970):
        self.countries_by_alpha3 = countries_by_alpha3
        self.countries_by_alpha3 = {k: v.name for k, v in self.countries_by_alpha3.items()}
        self.year_min = year_min

    def translate_country_code_to_country_name(self, serie_country_code_to_translate: pd.Series, raise_errors: bool):
        serie_country_translated = serie_country_code_to_translate.map(self.countries_by_alpha3)
        countries_no_translating = list(set(serie_country_code_to_translate[serie_country_translated.isnull()].values.tolist()))
        if serie_country_code_to_translate.isnull().sum() > 0:
            print("WARN : no translating found for countries %s. Please add it in iso3166.py" % countries_no_translating)
            if raise_errors:
                raise ValueError("ERROR : no translating found for countries %s" % countries_no_translating)

        return serie_country_translated


    def run(self, df_gdp_maddison, df_country):
        """
        Computes the total gdp maddisson for each year, each country and each geographic zone.
        """
        # convert countries names
        df_gdp_maddison = df_gdp_maddison.rename({"country": "country_raw_name"}, axis=1)
        df_gdp_maddison["country_apolitical_name"] = self.translate_country_code_to_country_name(df_gdp_maddison["country_raw_name"], raise_errors=True)
        df_gdp_maddison = df_gdp_maddison.dropna(subset=["country_apolitical_name"])
        df_gdp_maddison["country"] = CountryTranslatorFrenchToEnglish().run(df_gdp_maddison["country_apolitical_name"], raise_errors=False)

        # compute gdp from gdp per capita
        df_gdp_maddison["population"] = df_gdp_maddison["pop"] * 1000
        df_gdp_maddison["gdp"] = df_gdp_maddison["gdppc"] * df_gdp_maddison["population"]
        df_gdp_maddison = df_gdp_maddison[df_gdp_maddison["gdp"].notnull()]
        df_gdp_maddison["gdp_unit"] = "GDP (constant 2011 US$)"
        df_gdp_maddison["year"] = pd.to_numeric(df_gdp_maddison["year"])
        df_gdp_maddison = df_gdp_maddison[df_gdp_maddison["year"] >= self.year_min]

        # join with countries
        df_gdp_per_zone = (pd.merge(df_country, df_gdp_maddison, how='left', left_on='country', right_on='country')
                                    .groupby(['group_type', 'group_name', 'year', 'gdp_unit'])
                                    .agg({'gdp': 'sum'})
                                    .reset_index()
                                )
        
        # compute GDP per country
        df_gdp_per_country = df_gdp_maddison.copy()
        df_gdp_per_country = df_gdp_maddison.rename({"country": "group_name"}, axis=1)
        df_gdp_per_country["group_type"] = "country"
        df_gdp_per_country = df_gdp_maddison[["group_type", "group_name", "year", "gdp", "gdp_unit"]]

        # concatenate countries and zones populations
        df_gdp_per_zone_and_countries = pd.concat([df_gdp_per_zone, df_gdp_per_country], axis=0)
        df_gdp_per_zone_and_countries = df_gdp_per_zone_and_countries.sort_values(by=['group_type', 'group_name', 'year', "gdp_unit"])

        return df_gdp_per_zone_and_countries


class GdpWorldBankPerZoneAndCountryProcessor:

    def __init__(self, year_min=1970):
        self.year_min = year_min

    @staticmethod
    def melt_years(df: pd.DataFrame):
        return pd.melt(df, id_vars=["country", "country_code", "gdp_unit", "indicator_code"], 
                   var_name="year", 
                   value_name="gdp")

    def run(self, df_gdp_world_bank, df_gdp_constant, df_country):
        
        # rename columns and clean countries
        df_gdp_worldbank = pd.concat([df_gdp_world_bank, df_gdp_constant], axis=0)
        renamed_columns = {"Country Name": "country",
                           "Country Code": "country_code",
                           "Indicator Name": "gdp_unit",
                           "Indicator Code": "indicator_code"
                           }
        df_gdp_worldbank.rename(renamed_columns, axis="columns", inplace=True)
        df_gdp_worldbank["country"] = CountryTranslatorFrenchToEnglish().run(df_gdp_worldbank["country"], raise_errors=False)

        # Filter years, empty gdp values, and drop unused columns
        df_gdp_worldbank = df_gdp_worldbank[(df_gdp_worldbank["year"] >= self.year_min) & (df_gdp_worldbank["gdp"].notnull())]
        df_gdp_worldbank = df_gdp_worldbank[["country", "gdp_unit", "year", "gdp"]]

        # join with countries
        df_gdp_per_zone = (pd.merge(df_country, df_gdp_worldbank, how='left', left_on='country', right_on='country')
                                    .groupby(['group_type', 'group_name', 'year', 'gdp_unit'])
                                    .agg({'gdp': 'sum'})
                                    .reset_index()
                                )
        
        # compute GDP per country
        df_gdp_per_country = df_gdp_worldbank.copy()
        df_gdp_per_country = df_gdp_worldbank.rename({"country": "group_name"}, axis=1)
        df_gdp_per_country["group_type"] = "country"
        df_gdp_per_country = df_gdp_worldbank[["group_type", "group_name", "year", "gdp", "gdp_unit"]]

        # concatenate countries and zones populations
        df_gdp_per_zone_and_countries = pd.concat([df_gdp_per_zone, df_gdp_per_country], axis=0)
        df_gdp_per_zone_and_countries = df_gdp_per_zone_and_countries.sort_values(by=['group_type', 'group_name', 'year', "gdp_unit"])

        return df_gdp_per_zone_and_countries
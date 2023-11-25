import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish


class EoraCbaPerZoneAndCountryProcessor:

    def __init__(self, country_translations):
        self.country_translations = country_translations
        self.dict_translation_record_code_to_scope = {
            "PBA_GgCO2PerBnUSDGDP": "Territorial Emissions per GDP",
            "CBA_GgCO2PerBnUSDGDP": "Carbon Footprint per GDP",
            "PBA_GgCO2": "Territorial Emissions",
            "CBA_GgCO2": "Carbon Footprint",
            "PBA_tCO2perCap": "Territorial Emissions per Capita",
            "CBA_tCO2perCap": "Carbon Footprint per Capita"
        }
        self.dict_translation_record_code_to_co2_unit = {
            "PBA_GgCO2PerBnUSDGDP": "MtCO2 per Trillion $ GDP",
            "CBA_GgCO2PerBnUSDGDP": "MtCO2 per Trillion $ GDP",
            "PBA_GgCO2": "MtCO2",
            "CBA_GgCO2": "MtCO2",
            "PBA_tCO2perCap": "tCO2 per capita",
            "CBA_tCO2perCap": "tCO2 per capita"
        }

    @staticmethod
    def unstack_years_in_dataframe(df: pd.DataFrame):
        df = df.unstack().reset_index()
        df.columns = ["country", "record_code", "co2"]
        return df

    @staticmethod
    def convert_giga_units(df_eora_cba):
        df_eora_cba.loc[df_eora_cba["record_code"].str.contains["Gg"], "co2"] /= 1000
        return df_eora_cba

    def compute_scope_from_record_code(self, df_eora_cba):
        df_eora_cba["scope"] = df_eora_cba["record_code"].replace(self.dict_translation_record_code_to_scope)
        return df_eora_cba

    def compute_co2_unit_from_record_code(self, df_eora_cba):
        df_eora_cba["co2_unit"] = df_eora_cba["record_code"].replace(self.dict_translation_record_code_to_co2_unit)
        return df_eora_cba

    def run(self, df_eora_cba: pd.DataFrame, df_country: pd.DataFrame):
        """
        Computes the CO2 consumption stastistics per country and zone.
        :param df_eora_cba:
        :param df_country:
        :return:
        """
        # clean and filter countries
        df_eora_cba = df_eora_cba.rename({"Country": "country", "Record": "record_code"}, axis=1)
        df_eora_cba["country"] = CountryTranslatorFrenchToEnglish().run(df_eora_cba["Country"], raise_errors=False)
        df_eora_cba = df_eora_cba.dropna(subset=["country"], axis=1)
        df_eora_cba = self.convert_giga_units(df_eora_cba)

        # unstack the years.
        df_eora_cba = df_eora_cba.set_index(["country", "record_code"])
        df_eora_cba = self.unstack_years_in_dataframe(df_eora_cba)
        df_eora_cba["year"] = pd.to_numeric(df_eora_cba["year"])

        # create new columns scope, co2_unit and Source
        df_eora_cba["scope"] = self.compute_scope_from_record_code(df_eora_cba)
        df_eora_cba["scope"] = df_eora_cba["scope"].str.replace('Carbon Footprint', 'CO2 Footprint')
        df_eora_cba["co2_unit"] = self.compute_co2_unit_from_record_code(df_eora_cba)
        df_eora_cba["Source"] = "Eora"

        # join with countries
        df_eora_cba_per_zone = (pd.merge(df_country, df_eora_cba, on='country', how='left')
                                .groupby(['group_type', 'group_name', 'year', 'scope', 'co2_unit', 'source'])
                                .agg({'co2': "sum"})
                                .reset_index()
                                )

        # compute total co2 per country
        df_eora_cba_per_country = df_eora_cba.copy()
        df_eora_cba_per_country = df_eora_cba_per_country.rename({"country": "group_name"}, axis=1)
        df_eora_cba_per_country["group_type"] = "country"
        df_eora_cba_per_country = df_eora_cba_per_country[["group_type", "group_name", "year", "scope",
                                                           "co2", "co2_unit", "source"]]

        # concatenate countries and zones populations
        df_eora_cba_per_zone_and_countries = pd.concat([df_eora_cba_per_zone, df_eora_cba_per_country], axis=0)
        return df_eora_cba_per_zone_and_countries


class GcbPerZoneAndCountryProcessor:

    def __init__(self, country_translations):
        self.country_translations = country_translations

    @staticmethod
    def unstack_years_in_dataframe(df: pd.DataFrame):
        df = df.unstack().reset_index()
        df.columns = ["country", "co2"]
        return df

    def run(self, df_gcb_territorial: pd.DataFrame, df_gcb_cba: pd.DataFrame, df_country: pd.DataFrame):

        # transpose and unstack years for the two datasets
        df_gcb_territorial = df_gcb_territorial.transpose()
        df_gcb_territorial = self.unstack_years_in_dataframe(df_gcb_territorial)
        df_gcb_cba = df_gcb_cba.transpose()
        df_gcb_cba = self.unstack_years_in_dataframe(df_gcb_cba)

        # concat the two dataframes and filter time and missing values
        df_gcb_territorial["scope"] = "Territorial Emissions"
        df_gcb_cba["scope"] = "Carbon Footprint"
        df_gcb = pd.concat([df_gcb_territorial, df_gcb_cba], axis=0)
        df_gcb["year"] = pd.to_numeric(df_gcb["year"])
        df_gcb = df_gcb[df_gcb["year"] >= 1990]
        df_gcb = df_gcb.dropna(subset=["co2"], axis=1)
        df_gcb["country"] = CountryTranslatorFrenchToEnglish().run(df_gcb["Country"], raise_errors=False)
        df_gcb = df_gcb.dropna(subset=["country"], axis=1)

        # create new columns scope, co2_unit and Source
        df_gcb["co2"] *= 3.664
        df_gcb["co2_unit"] = "MtCO2"
        df_gcb["Source"] = "Global Carbon Budget"
        df_gcb["scope"] = df_gcb["scope"].str.replace('Carbon Footprint', 'CO2 Footprint')

        # join with countries
        df_gcb_per_zone = (pd.merge(df_country, df_gcb, on='country', how='left')
                           .groupby(['group_type', 'group_name', 'year', 'scope', 'co2_unit', 'source'])
                           .agg({'co2': "sum"})
                           .reset_index()
                           )

        # compute total co2 per country
        df_gcb_per_country = df_gcb.copy()
        df_gcb_per_country = df_gcb_per_country.rename({"country": "group_name"}, axis=1)
        df_gcb_per_country["group_type"] = "country"
        df_gcb_per_country = df_gcb_per_country[["group_type", "group_name", "year", "scope",
                                                 "co2", "co2_unit", "source"]]

        # concatenate countries and zones populations
        df_gcb_per_zone_and_countries = pd.concat([df_gcb_per_zone, df_gcb_per_country], axis=0)
        return df_gcb_per_zone_and_countries

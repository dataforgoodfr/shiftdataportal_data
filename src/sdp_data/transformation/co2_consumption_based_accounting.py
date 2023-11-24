import pandas as pd

# TODO - mutualiser la traduction dans une classe à part.


class EoraCbaWithZonesProcessor:

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

    def convert_countries_from_french_to_english(self, df_eora_cba):  # TODO - ajouter un test au fil de l'eau pour vérifier si chaque pays a bien reçu une correspondance dans le dictionnaire.
        df_eora_cba["country"] = df_eora_cba["country"].replace(self.country_translations)
        return df_eora_cba

    @staticmethod
    def unstack_dataframe_to_serie(df: pd.DataFrame):
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

        :param df_eora_cba:
        :param df_country:
        :return:
        """
        # clean and filter countries
        df_eora_cba = df_eora_cba.rename({"Country": "country", "Record": "record_code"}, axis=1)
        df_eora_cba["country"] = self.convert_countries_from_french_to_english(df_eora_cba["Country"])
        df_eora_cba = df_eora_cba[~df_eora_cba["country"].contains("NOT FOUND")]
        df_eora_cba = self.convert_giga_units(df_eora_cba)

        # unstack years on the row axis.
        df_eora_cba = df_eora_cba.set_index(["country", "record_code"])
        df_eora_cba = self.unstack_dataframe_to_serie(df_eora_cba)
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





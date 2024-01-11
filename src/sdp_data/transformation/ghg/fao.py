import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish
from sdp_data.transformation.demographic.countries import StatisticsPerCountriesAndZonesJoiner


class FaoDataProcessor:
    def __init__(self):
        self.dict_cols_to_rename = {
            "Area": "country",
            "Item": "sector",
            "Year": "year",
            "Unit": "ghg_unit",
            "Value": "ghg",
            "Element": "gas_before",
            "1": "gas",
        }
        self.list_sectors_to_exclude = [
            "Energy total",
            "Forest",
            "Sources total",
            "Land use sources",
            "Sources total excl. AFOLU",
            "Land Use total",
        ]
        self.dict_translation_sectors = {
            "Residential, commercial, institutional and AFF": "Residential",
            "Industrial processes and product use": "Industrial processes",
            "Agriculture total": "Agriculture",
            "Other sources": "Other",
            "Energy (energy, manufacturing and construction industries and fugitive emissions)": "Energy",
        }

    def translate_country_code_to_country_name(self, serie_country_code_to_translate: pd.Series, raise_errors: bool):
        """
        Translate country code to country name
        """
        serie_country_translated = serie_country_code_to_translate.map(self.countries_by_name)
        countries_no_translating = list(set(serie_country_code_to_translate[serie_country_translated.isnull()].values.tolist()))
        if serie_country_translated.isnull().sum() > 0:
            print("WARN : no translating found for countries %s. Please add it in iso3166.py" % countries_no_translating)
            if raise_errors:
                raise ValueError("ERROR : no translating found for countries %s" % countries_no_translating)

        return serie_country_translated

    def run(self, df_fao: pd.DataFrame, df_country):
        """

        :return:
        """
        # clean countries
        print(df_fao["Area"])
        df_fao["Area"] = CountryTranslatorFrenchToEnglish().run(df_fao["Area"], raise_errors=False)
        df_fao["Area"] = df_fao[df_fao["Area"] != "Delete"]
        print(df_fao["Area"])

        # clean and add new columns
        df_fao = df_fao.rename(self.dict_cols_to_rename, axis=1)
        df_fao["ghg_unit"] = "MtCO2eq"
        df_fao["source"] = "FAO"
        df_fao["ghg"] = df_fao["ghg"] * 0.001
        print(df_fao["Area"])

        # Extract gas
        df_fao = df_fao[~df_fao["gas_before"].str.contains("Share")]
        df_fao = df_fao[~df_fao["gas_before"].isin(["Emissions (CO2eq)", "share"])]
        df_fao["gas"] = df_fao["gas_before"].str.split(" ").str[-1]
        df_fao["gas"] = df_fao["gas"].replace({"F-gases": "F-Gases"})
        # TODO - ajouter un vrai module commun de traduction des gaz.

        # filter on the right sectors and countries
        # TODO - re-challenger les filtres mis sur les pays ou les secteurs.
        df_fao = df_fao.dropna(subset=["country"])
        df_fao = df_fao[df_fao["country"] != "China"]
        df_fao["country"] = df_fao["country"].replace({"China, mainland": "China"})
        df_fao = df_fao[~df_fao["sector"].isin(self.list_sectors_to_exclude)]
        df_fao["sector"] = df_fao["sector"].replace(self.dict_translation_sectors)
        df_fao = df_fao.drop(["Area Code", "Item Code", "Element Code", "Year Code", "Flag", "gas_before"], axis=1)
        # TODO - ajouter un vrai module commun de traduction de secteurs.
        # TODO - ajouter le filtrage Regex.

        # join with countries
        list_cols_group_by = ["group_type", "group_name", "year", "sector", "gas", "source", "ghg_unit"]
        dict_agg = {"ghg": "sum"}
        df_fao_per_country_and_zones = StatisticsPerCountriesAndZonesJoiner().run(df_fao, df_country, list_cols_group_by, dict_agg)

        # format and return
        df_fao_per_country_and_zones = df_fao_per_country_and_zones.sort_values(list_cols_group_by)
        df_fao_per_country_and_zones = df_fao_per_country_and_zones[list_cols_group_by + ["ghg"]]
        return df_fao_per_country_and_zones

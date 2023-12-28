import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish, SectorTranslator
from sdp_data.utils.iso3166 import countries_by_name
from sdp_data.transformation.ghg.edgar import EdgarCleaner
from sdp_data.transformation.demographic.countries import StatisticsPerCountriesAndZonesJoiner
import numpy as np

class FaoDataProcessor:

    def __init__(self):
        self.countries_by_name = countries_by_name
        self.countries_by_name = {k: v.name for k, v in self.countries_by_name.items()}
        self.list_sectors_to_exclude = ["Energy total", "Forest", "Sources total", "Land use sources",
                                        "Sources total excl. AFOLU", "Land Use total"]
        self.dict_translation_sectors = {
            "Residential": "Residential",
            "Industrial processes and product use": "Industrial processes",
            "Agriculture total": "Agriculture",
            "Other sources": "Other",
            "Energy (energy, manufacturing and construction industries and fugitive emissions)": "Energy (energy, manufacturing and construction industries and fugitive emissions)"
        }

    def translate_country_code_to_country_name(self, serie_country_code_to_translate: pd.Series, raise_errors: bool):
        serie_country_translated = serie_country_code_to_translate.map(self.countries_by_name)
        countries_no_translating = list(set(serie_country_code_to_translate[serie_country_translated.isnull()].values.tolist()))
        if serie_country_code_to_translate.isnull().sum() > 0:
            print("WARN : no translating found for countries %s. Please add it in iso3166.py" % countries_no_translating)
            if raise_errors:
                raise ValueError("ERROR : no translating found for countries %s" % countries_no_translating)

        return serie_country_translated

    def run(self, df_fao: pd.DataFrame, df_country):
        """

        :return:
        """
        # clean dataframe
        # df_fao["Area"] = df_fao["Area"].fillna(self.translate_country_code_to_country_name(df_fao["Area Code"], raise_errors=False))
        # df_fao["Area"] = self.translate_country_code_to_country_name(df_fao["Area"], raise_errors=False)
        df_fao = df_fao.rename({"Area": "country", "Item": "sector", "Year": "year", "Unit": "ghg_unit",
                                "Value": "ghg", "Element": "gas_before", "1": "gas"}, axis=1)
        df_fao["ghg_unit"] = "MtCO2eq"
        df_fao["source"] = "FAO"
        df_fao["ghg"] = df_fao["ghg"] * 0.001

        # Extract gas
        df_fao = df_fao[df_fao["gas_before"].str.contains("Share") == False]
        df_fao = df_fao[df_fao["gas_before"] != "Emissions (CO2eq)"]
        df_fao["gas"] = df_fao["gas_before"].str.split(' ').str[-1]
        df_fao["gas"] = df_fao["gas"].replace({"F-gases": "F-Gases"})  # TODO - ajouter un vrai module commun de traduction des gaz.

        # filter on the right sectors and countries
        df_fao = df_fao.dropna(subset=["country"])
        df_fao = df_fao[df_fao["country"] != "China"]  # TODO - re-challenger les filtres mis sur les pays ou les secteurs.
        df_fao["country"] = df_fao["country"].replace({"China, mainland": "China"})
        df_fao = df_fao[df_fao["sector"].isin(self.list_sectors_to_exclude)]
        df_fao = df_fao.drop(["Area Code", "Item Code", "Element Code", "Year Code", "Flag", "gas_before"], axis=1)
        df_fao["sector"] = df_fao["sector"].replace(self.dict_translation_sectors)  # TODO - ajouter un vrai module commun de traduction de secteurs.
        # TODO - ajouter le filtrage Regex.

        # join with countries
        df_fao_per_zones = (
            pd.merge(df_country, df_fao, how='left', left_on='country', right_on='country')
            .groupby(['group_type', 'group_name', 'year', "sector", "gas", "ghg_unit", "source"])
            .agg({'ghg': 'sum'})
            .reset_index()
        )

        # compute FAO per country
        df_fao_per_country = df_fao.copy()
        df_fao_per_country = df_fao_per_country.rename({"country": "group_name"}, axis=1)
        df_fao_per_country["group_type"] = "country"
        df_fao_per_country = df_fao_per_country[["source", "group_type", "group_name", "year", "sector", "gas", "ghg", "ghg_unit"]]
        df_fao_per_country_and_zones = pd.concat([df_fao_per_zones, df_fao_per_country], axis=0)

        return df_fao_per_country_and_zones
    
class GhgAllDatasetsProcessor:

    def compute_pik_edgar(self, df_pik_clean, df_edgar_clean):

        # filter Edgat on relevant sectors
        df_edgar_filter_sector = df_edgar_clean[df_edgar_clean["sector"].isin('Transport','Electricity & Heat','Other Energy')]

        # Select rows from with sector = 'Industry and Construction' and merge
        df_edgar_industry = df_edgar_clean[df_edgar_clean['sector'] == 'Industry and Construction']
        df_pik_industry = df_pik_clean[df_pik_clean['sector'] == 'Industry and Construction']
        df_union_industry = pd.merge(df_edgar_industry, df_pik_industry, on=['country', 'year', 'gas'], suffixes=('_edgar', '_pik'))
        df_union_industry['ghg'] = df_union_industry['ghg_edgar'] - df_union_industry['ghg_pik']
        df_union_industry = df_union_industry[['country', 'sector', 'gas', 'year', 'ghg', 'ghg_unit']]

        # finally Concatenate df_c1 and df_union
        df_result = pd.concat([df_edgar_filter_sector, df_union_industry])
        return df_result
        

    def run(self, df_edgar_clean, df_unfccc_annex_clean, df_pik_clean, df_country):
        """
        """
        # merge PIK data and UNFCC data and Edgar data
        df_pik_unfccc = pd.concat([df_pik_clean, df_unfccc_annex_clean], axis=0)

        # merge PIK data and Edgar data
        df_pik_edgar_stacked = pd.concat([df_pik_unfccc, df_edgar_clean], axis=0)
        df_pik_edgar_merge = self.compute_pik_edgar(df_pik_clean, df_edgar_clean)

        # stack Unfccc and Edgar data and merge with countries
        df_pik_unfccc["source"] = "unfccc"
        df_edgar_clean["source"] = "edgar_cleaned2"
        df_pik_edgar_stacked = pd.concat([df_pik_unfccc, df_edgar_clean], axis=0)

        list_group_by = ["group_type", "group_name", "source", "year", "sector", "gas", "ghg_unit"]
        dict_aggregation = {"ghg": "sum"}
        df_pik_edgar_stacked_per_zones_and_countries = StatisticsPerCountriesAndZonesJoiner().run(df_pik_edgar_stacked, df_country, list_group_by, dict_aggregation)


        groupby_by_gas = [["source", "group_type", "group_name", "year", "gas"]]
        df_ghg_edunf_by_gas = df_ghg_edunf_by_gas.groupby(groupby_by_gas)





        

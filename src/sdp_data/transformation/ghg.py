import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish, SectorTranslator
from sdp_data.utils.iso3166 import countries_by_name
import numpy as np


class PikCleaner:

    def __init__(self):
        self.list_countries_to_remove = ["World", "Non-Annex-I Parties to the Convention",  "Annex-I Parties to the Convention",
                                         "BASIC countries (Brazil, South Africa, India and China)", "Umbrella Group",
                                         "Umbrella Group (28)", "Least Developed Countries", "European Union (28)",
                                         "Alliance of Small Island States (AOSIS)"]
        self.list_sectors_to_replace = {
            "Other": "Other Sectors",
            "Industrial Processes and Product Use": "Industry and Construction"
        }

    @staticmethod
    def melt_years(df_pik: pd.DataFrame):
        return pd.melt(df_pik, id_vars=["country", "source", "sector", "gas", "ghg_unit"], var_name='year', value_name='ghg')

    def run(self, df_pik: pd.DataFrame):
        """
        Cleans the PIK dataset.
        :param df_pik:
        :return:
        """
        # cleaning data
        print("----- Clean PIK dataset")
        df_pik = df_pik.rename({"Country": "country", "Data source": "source", "Sector": "sector",
                                "Gas": "gas", "Unit": "ghg_unit"}, axis=1)
        df_pik = df_pik[df_pik["sector"] != "Total excluding LULUCF"]
        df_pik["sector"] = df_pik["sector"].replace(self.list_sectors_to_replace)
        df_pik = df_pik[~df_pik["country"].isin(self.list_countries_to_remove)]

        # melt years
        df_pik = self.melt_years(df_pik)
        # df_pik = df_pik.dropna(subset=["ghg"])  # TODO - jeter valeurs manquante ? Legacy Dataiku

        # translate countries
        df_pik["country"] = CountryTranslatorFrenchToEnglish().run(df_pik["country"], raise_errors=False)
        df_pik = df_pik.dropna(subset=["country"])
        df_pik = df_pik[df_pik["country"] != "Delete"]

        return df_pik


class UnfcccCleaner:

    def __init__(self):
        pass

    @staticmethod
    def melt_years_and_gas(df_unfccc_stacked: pd.DataFrame):
        return pd.melt(df_unfccc_stacked, id_vars=["country", "sector"], var_name='year_gas', value_name='ghg')

    def run(self, df_unfccc_annex_1: pd.DataFrame, df_unfccc_annex_2: pd.DataFrame):
        """
        Cleans the Unfccc data
        :param df_unfccc_annex_1: (dataframe) contains the first part of Unfccc data.
        :param df_unfccc_annex_2: (dataframe) contains the second part of Unfccc data.
        :return: dataframe Unfccc data cleaned.
        """
        list_cols_annex_1 = [col for col in df_unfccc_annex_1.columns if "Last Inventory" not in col]
        list_cols_annex_2 = [col for col in df_unfccc_annex_2.columns if "Last Inventory" not in col]
        df_unfccc_annex_1 = df_unfccc_annex_1[list_cols_annex_1]
        df_unfccc_annex_2 = df_unfccc_annex_2[list_cols_annex_2]
        df_unfccc_stacked = pd.concat([df_unfccc_annex_1, df_unfccc_annex_2], axis=0)

        # clean dataset and melt
        df_unfccc_stacked = df_unfccc_stacked.rename({"Party": "country", 'Category \ Unit': "sector"}, axis=1)
        df_unfccc_stacked = self.melt_years_and_gas(df_unfccc_stacked)
        df_unfccc_stacked["sector"] = SectorTranslator().translate_sector_unfccc_data(df_unfccc_stacked["sector"], raise_errors=False)
        df_unfccc_stacked["country"] = CountryTranslatorFrenchToEnglish().run(df_unfccc_stacked["country"], raise_errors=False)

        # split years and gas
        df_unfccc_stacked["year"] = df_unfccc_stacked["year_gas"].str.split(" ").map(lambda x: x[0])
        df_unfccc_stacked["gas"] = df_unfccc_stacked["year_gas"].str.split(" ").map(lambda x: x[1])
        df_unfccc_stacked = df_unfccc_stacked.drop("year_gas", axis=1)

        # convert ghg and drop missing values # TODO - ajouter la conversion des données non CO2 comme dans les données Edgar ?
        df_unfccc_stacked["ghg"] = 0.001 * pd.to_numeric(df_unfccc_stacked["ghg"], errors="coerce")
        df_unfccc_stacked["ghg_unit"] = "MtCO2eq"
        df_unfccc_stacked = df_unfccc_stacked.dropna(subset=["country", "ghg"], axis=0)

        return df_unfccc_stacked


class EdgarCleaner:

    def __init__(self):
        self.dict_gas_to_replace = {"PFC": "F-Gas", "HFC": "F-Gas", "SF6": "F-Gas"}

    @staticmethod
    def melt_years(df_edgar_stacked: pd.DataFrame):
        return pd.melt(df_edgar_stacked, id_vars=["country", "sector", "gas"], var_name='year', value_name='ghg')

    @staticmethod
    def convert_ghg_with_gas(ghg, gas):
        if gas == "N2O":
            return ghg * 298
        elif gas == "CH4":
            return ghg * 25
        elif gas == "CO2":
            return ghg
        elif gas in ["SF6", "HFC", "PFC", "F-Gas"]:  # TODO - ajouter une conversion pour ces gas ?
            return ghg
        else:
            raise ValueError("ERR : unknown gas : %s" % gas)

    @staticmethod
    def custom_sum_sentive_nan(series):
        if series.isna().all():
            return np.nan
        return series.sum()

    def run(self, df_edgar_gases, df_edgar_n2o, df_edgar_ch4, df_edgar_co2_short_cycle, df_edgar_co2_short_without_cycle):
        """
        Aggregates and cleans the different EDGAR data files related to gas statistics
        :param df_edgar_n2o: (dataframe) containing the N20 data.
        :param df_edgar_ch4:(dataframe) containing the CH4 data.
        :param df_edgar_co2_short_cycle: (dataframe) containing the CO2 short-cycle data.
        :param df_edgar_co2_short_without_cycle: (dataframe) containing the CO2 without short-cycle data.
        :param df_edgar_gases:
        :return:
        """
        # stack the different gas together
        df_edgar_n2o["gas"] = "N2O"
        df_edgar_ch4["gas"] = "CH4"
        df_edgar_co2_short_cycle["gas"] = "CO2"
        df_edgar_co2_short_without_cycle["gas"] = "CO2"
        df_edgar_stacked = pd.concat([df_edgar_n2o, df_edgar_ch4, df_edgar_co2_short_cycle,
                                      df_edgar_co2_short_without_cycle, df_edgar_gases])

        # melt years and create new columns
        df_edgar_stacked = df_edgar_stacked.rename({"Name": "country", "IPCC_description": "sector"}, axis=1)
        df_edgar_stacked = df_edgar_stacked.drop(["IPCC-Annex", "ISO_A3", "World Region", "IPCC"], axis=1)
        df_edgar_stacked = self.melt_years(df_edgar_stacked)
        df_edgar_stacked["sector"] = SectorTranslator().translate_sector_edgar_data(df_edgar_stacked["sector"], raise_errors=False)

        # convert ghg and drop missing values # TODO - ajouter la conversion des données non CO2 comme dans les données Edgar ?
        df_edgar_stacked["gas"] = df_edgar_stacked["gas"].replace(self.dict_gas_to_replace)
        df_edgar_stacked["ghg"] = 0.001 * pd.to_numeric(df_edgar_stacked["ghg"], errors="coerce")
        df_edgar_stacked["ghg"] = df_edgar_stacked.apply(lambda row: self.convert_ghg_with_gas(row["ghg"], row["gas"]), axis=1)
        df_edgar_stacked["ghg_unit"] = "MtCO2eq"

        # some all ghg and clean countries
        list_groupby = ["country", "sector", "gas", "year", "ghg_unit"]
        df_edgar_stacked = df_edgar_stacked.groupby(list_groupby)["ghg"].agg(self.custom_sum_sentive_nan).reset_index()  # TODO - corriger les valeurs manquantes ?
        df_edgar_stacked["country"] = CountryTranslatorFrenchToEnglish().run(df_edgar_stacked["country"], raise_errors=False)
        df_edgar_stacked["country"] = df_edgar_stacked["country"].replace({"Reunion": "Réunion"})  # TODO - à corriger dans fichier translation
        df_edgar_stacked = df_edgar_stacked.dropna(subset=["country"])
        df_edgar_stacked["year"] = df_edgar_stacked["year"].astype(int)

        return df_edgar_stacked


class CaitProcessor:

    def __init__(self):
        self.list_gases = ['Total CH4 (MtCO2e)', 'Total CH4 (including LUCF) (MtCO2e)',
                           'Total CO2 (excluding LUCF) (MtCO2)', 'Total CO2 (including LUCF) (MtCO2)',
                           'Total N2O (MtCO2e)', 'Total N2O (including LUCF) (MtCO2e)',
                           'Total F-Gas (MtCO2e)']

        self.list_sectors = ['Other Fuel Combustion (MtCO2e)', 'Fugitive Emissions (MtCO2e)', 'Energy (MtCO2e)', 'Electricity/Heat (MtCO2)',
                             'Waste (MtCO2e)', 'Bunker Fuels (MtCO2)', 'Agriculture (MtCO2e)', 'Manufacturing/Construction (MtCO2)',
                             'LUCF (MtCO2)', 'Transportation (MtCO2)', 'Industrial Processes (MtCO2e)']

        self.gases_including_lucf = {
            'Total CH4 (MtCO2e)': False,
            'Total CH4 (including LUCF) (MtCO2e)': True,
            'Total CO2 (excluding LUCF) (MtCO2)': False,
            'Total CO2 (including LUCF) (MtCO2)': True,
            'Total N2O (MtCO2e)': False,
            'Total N2O (including LUCF) (MtCO2e)': True,
            'Total F-Gas (MtCO2e)': False
        }

        self.gases_new_name = {
            'Total CH4 (MtCO2e)': "CH4",
            'Total CH4 (including LUCF) (MtCO2e)': "CH4",
            'Total CO2 (excluding LUCF) (MtCO2)': "CO2",
            'Total CO2 (including LUCF) (MtCO2)': "CO2",
            'Total N2O (MtCO2e)': "N2O",
            'Total N2O (including LUCF) (MtCO2e)': "N2O",
            'Total F-Gas (MtCO2e)': "F-Gases"
        }

    def melt_gases_and_sectors(self, df_calt: pd.DataFrame):
        """

        :param df_calt:
        :return:
        """
        # compute and melt for each gas.
        list_df_gas_melted_gas = []
        for gas in self.list_gases:
            df_calt_gas = df_calt[["Country", "Year", gas]]
            df_calt_gas = df_calt_gas.rename({gas: "ghg"}, axis=1)
            df_calt_gas["gas"] = self.gases_new_name[gas]
            df_calt_gas["including_lucf"] = self.gases_including_lucf[gas]
            list_df_gas_melted_gas.append(df_calt_gas.copy(deep=True))
            if gas == 'Total F-Gas (MtCO2e)':  # For F-Gas we only have 1 line so we duplicate it for including and excluding lucf
                df_calt_gas_with_lucf = True
                list_df_gas_melted_gas.append(df_calt_gas_with_lucf.copy(deep=True))
        df_gas_melted = pd.concat(list_df_gas_melted_gas, axis=0)

        # compute and melt for each sector.
        list_df_gas_melted_sector = []
        for sector in self.list_sectors:
            df_calt_sector = df_calt[["Country", "Year", sector]]
            df_calt_sector = df_calt_sector.rename({sector: "ghg"}, axis=1)
            df_calt_sector["sector"] = sector.split(" (MtCO2")[0]
            list_df_gas_melted_sector.append(df_calt_sector.copy(deep=True))
        df_sector_melted = pd.concat(list_df_gas_melted_sector, axis=0)
        return pd.concat([df_gas_melted, df_sector_melted], axis=0)

    @staticmethod
    def remove_energy_sector(df_calt):
        # Energy is already counted in the 5 subsectors : Electricity/Heat, Manufacturing/Construction, ...
        # To avoid double accounting we exclude it
        # see : https://sites.google.com/site/climateanalysisindicatorstool/cait-international-8-0/sector-definitions
        return df_calt[df_calt["sector"] != "Energy"]

    @staticmethod
    def remove_lucf_sector(df_calt):
        # Remove LUCF sector because values can be negative and we don't have a solution at the moment to visualize it
        return df_calt[df_calt["sector"] != "LUCF"]

    @staticmethod
    def compute_cait_data_stacked_sector(df_cait_group_by_sector, df_cait_prepared):
        """

        :param df_cait_group_by_sector:
        :param df_cait_prepared:
        :return:
        """
        list_cols_to_select = ["group_type", "group_name", "year", "sector", "ghg", "ghg_unit"]
        df_cait_group_by_sector_not_world = df_cait_group_by_sector[df_cait_group_by_sector["group_name"] != "World"]
        df_cait_group_by_sector_not_world = df_cait_group_by_sector_not_world[list_cols_to_select]

        # compute statistics per country
        df_cait_prepared_not_world = df_cait_prepared[(df_cait_prepared["sector"].notnull()) & (df_cait_prepared["country"] != "World")]
        df_cait_prepared_not_world["group_type"] = 'country'
        df_cait_prepared_not_world = df_cait_prepared_not_world[list_cols_to_select]

        # compute statistics per zone
        df_cait_prepared_world = df_cait_prepared[(df_cait_prepared["sector"].notnull()) & (df_cait_prepared["country"] == "World")]
        df_cait_prepared_world["group_type"] = 'zone'
        df_cait_prepared_world = df_cait_prepared_world[list_cols_to_select]

        return pd.concat([df_cait_group_by_sector_not_world, df_cait_prepared_not_world, df_cait_prepared_world], axis=0)

    @staticmethod
    def compute_cait_data_stacked_gas(df_cait_group_by_country, df_cait_prepared):
        """

        :param df_cait_group_by_country:
        :param df_cait_prepared:
        :return:
        """
        list_cols_to_select = ["group_type", "group_name", "year", "including_lucf", "gas", "ghg", "ghg_unit"]
        df_cait_group_by_gas_not_world = df_cait_group_by_country[df_cait_group_by_country["group_name"] != "World"]
        df_cait_group_by_gas_not_world = df_cait_group_by_gas_not_world[list_cols_to_select]

        # compute statistics per country
        df_cait_prepared_not_world = df_cait_prepared[(df_cait_prepared["gas"].notnull()) & (df_cait_prepared["country"] != "World")]
        df_cait_prepared_not_world["group_type"] = 'country'
        df_cait_prepared_not_world = df_cait_prepared_not_world[list_cols_to_select]

        # compute statistics per zone
        df_cait_prepared_world = df_cait_prepared[(df_cait_prepared["country"].notnull()) & (df_cait_prepared["country"] == "World")]
        df_cait_prepared_world["group_type"] = 'zone'
        df_cait_prepared_world = df_cait_prepared_world[list_cols_to_select]

        return pd.concat([df_cait_group_by_gas_not_world, df_cait_prepared_not_world, df_cait_prepared_world], axis=0)


    def run(self, df_cait: pd.DataFrame, df_country: pd.DataFrame):
        # clean dataframe
        df_cait = df_cait.rename({"Country": "country", "Year": "year"}, axis=1)
        df_cait["country"] = CountryTranslatorFrenchToEnglish().run(df_cait["country"], raise_errors=False)
        df_cait_prepared = self.melt_gases_and_sectors(df_cait)
        df_cait_prepared = self.remove_energy_sector(df_cait_prepared)
        df_cait_prepared = self.remove_lucf_sector(df_cait_prepared)
        print(df_cait_prepared.shape)

        # compute total CAIT per sector
        df_cait_and_countries = df_country.merge(df_cait_prepared, how="left", left_on="country", right_on="country")
        df_cait_group_by_sector = df_cait_and_countries[df_cait_and_countries["sector"].notnull()]
        df_cait_group_by_sector = df_cait_group_by_sector.groupby(["group_type", "group_name", "year", "sector"])
        df_cait_group_by_sector = df_cait_group_by_sector.agg({'ghg': "sum", "ghg_unit": "first"}).reset_index(drop=True)

        # compute total CAIT per contry
        df_cait_group_by_country = df_cait_and_countries[df_cait_and_countries["sector"].isnull()]
        df_cait_group_by_country = df_cait_group_by_country.groupby(["group_type", "group_name", "year", "including_lucf", "gas"])
        df_cait_group_by_country = df_cait_group_by_country.agg({'ghg': "sum", "ghg_unit": "first"}).reset_index(drop=True)

        # compute CAIT stacked  # TODO - code smell, code trop complexe à refactorer
        df_cait_sector_stacked = self.compute_cait_data_stacked_sector(df_cait_group_by_sector, df_cait_prepared)
        df_cait_gas_stacked = self.compute_cait_data_stacked_sector(df_cait_group_by_country, df_cait_prepared)

        return df_cait_sector_stacked, df_cait_gas_stacked


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
        df_fao["Area"] = self.translate_country_code_to_country_name(df_fao["Area"], raise_errors=False)
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

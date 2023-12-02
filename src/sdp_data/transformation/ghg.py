import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish, SectorTranslator


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
        else:
            raise ValueError("ERR : unknown gas : %s" % gas)

    def run(self, df_edgar_gases, df_edgar_n2o, df_edgar_ch4, df_edgar_co2_short_cycle, df_edgar_co2_short_without_cycle):
        """

        :param df_edgar_n2o:
        :param df_edgar_ch4:
        :param df_edgar_co2_short_cycle:
        :param df_edgar_co2_short_without_cycle:
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
        df_edgar_stacked["country"] = CountryTranslatorFrenchToEnglish().run(df_edgar_stacked["country"], raise_errors=False)

        # convert ghg and drop missing values # TODO - ajouter la conversion des données non CO2 comme dans les données Edgar ?
        df_edgar_stacked["ghg"] = 0.001 * pd.to_numeric(df_edgar_stacked["ghg"], errors="coerce")
        df_edgar_stacked["ghg"] = df_edgar_stacked.apply(lambda row: self.convert_ghg_with_gas(row["ghg"], row["gas"]), axis=1)
        df_edgar_stacked["ghg_unit"] = "MtCO2eq"

        # some all ghg
        list_groupby = ["country", "sector", "gas", "year", "ghg_unit"]
        df_edgar_stacked = df_edgar_stacked.groupby(list_groupby)["ghg"].sum()

        return df_edgar_stacked

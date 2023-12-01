import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish


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
        self.list_sectors_to_replace =
    @staticmethod
    def melt_years_and_gas(df_unfccc_stacked: pd.DataFrame):
        return pd.melt(df_unfccc_stacked, id_vars=["country", "source", "sector", "gas", "ghg_unit"], var_name='year', value_name='ghg')


    def run(self, df_unfccc_annex_1: pd.DataFrame, df_unfccc_annex_2: pd.DataFrame):
        """

        :param df_unfccc_annex_1:
        :param df_unfccc_annex_2:
        :return:
        """
        list_cols_annex_1 = [col for col in df_unfccc_annex_1.columns if "Last Inventory" not in col]
        list_cols_annex_2 = [col for col in df_unfccc_annex_2.columns if "Last Inventory" not in col]
        df_unfccc_annex_1 = df_unfccc_annex_1[list_cols_annex_1]
        df_unfccc_annex_2 = df_unfccc_annex_2[list_cols_annex_2]
        df_unfccc_stacked = pd.concat([df_unfccc_annex_1, df_unfccc_annex_2], axis=0)

        # clean dataset
        df_pik = df_pik.rename({"Party": "country", 'Category \ Unit': "sector"}, axis=1)





def addition(a, b):
    return a + b



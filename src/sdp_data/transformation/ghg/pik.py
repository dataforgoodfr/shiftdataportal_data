import pandas as pd
from src.sdp_data.utils.translation import CountryTranslatorFrenchToEnglish, CountryIsoCodeTranslator
from src.sdp_data.utils.format import StatisticsDataframeFormatter


class PikCleaner:

    def __init__(self):
        self.list_countries_to_remove = ["World", "Non-Annex-I Parties to the Convention",  "Annex-I Parties to the Convention",
                                         "BASIC countries (Brazil, South Africa, India and China)", "Umbrella Group",
                                         "Umbrella Group (28)", "Least Developed Countries", "European Union (28)",
                                         "Alliance of Small Island States (AOSIS)"]
        self.list_sectors_to_replace = {}

    @staticmethod
    def convert_ghg_unit(df_pik: pd.DataFrame):
        """
        Convert the GHG unit to CO2eq.
        :param df_pik:
        :return:
        """
        df_pik["ghg"] *= 1000
        df_pik["ghg_unit"] = "MtCO2eq"
        return df_pik

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
        print("\n----- Clean PIK dataset")
        df_pik = df_pik.rename({"area (ISO3)": "country", "category (IPCC2006_PRIMAP)": "sector",
                                "scenario (PRIMAP-hist)": "scenario",
                                "entity": "gas", "unit": "ghg_unit"}, axis=1)
        df_pik = df_pik[df_pik["scenario"] == "HISTTP"]
        df_pik = df_pik[df_pik["sector"] != "Total excluding LULUCF"]  # TODO - revoir traitement des secteurs
        df_pik["sector"] = df_pik["sector"].replace(self.list_sectors_to_replace)
        df_pik["country"] = CountryIsoCodeTranslator().run(df_pik["country"], raise_errors=True)
        df_pik["source"] = "PIK"
        df_pik = df_pik[~df_pik["country"].isin(self.list_countries_to_remove)]
        df_pik = df_pik.drop(columns=["provenance", "scenario"])

        # melt years and converts units
        df_pik = self.melt_years(df_pik)
        df_pik = self.convert_ghg_unit(df_pik)

        # translate countries
        df_pik["country"] = CountryTranslatorFrenchToEnglish().run(df_pik["country"], raise_errors=False)
        df_pik = df_pik.dropna(subset=["country"])
        df_pik = df_pik[df_pik["country"] != "Delete"]
        df_pik = StatisticsDataframeFormatter.select_and_sort_values(df_pik, "ghg", round_statistics=5)

        return df_pik

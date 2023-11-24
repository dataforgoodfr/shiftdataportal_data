import pandas as pd

# TODO - mutualiser la traduction dans une classe à part.


class EoraCbaWithZonesProcessor:

    def __init__(self, country_translations):
        self.country_translations = country_translations

    def convert_countries_from_french_to_english(self, df_eora_cba):  # TODO - ajouter un test au fil de l'eau pour vérifier si chaque pays a bien reçu une correspondance dans le dictionnaire.
        df_eora_cba["country"] = df_eora_cba["country"].replace(self.country_translations)
        return df_eora_cba

    def run(self, df_eora_cba: pd.DataFrame, df_country: pd.DataFrame):
        """

        :param df_eora_cba:
        :param df_country:
        :return:
        """
        df_eora_cba = df_eora_cba.rename({"Country": "country", "Record": "record_code"}, axis=1)
        df_eora_cba["country"] = self.convert_countries_from_french_to_english(df_eora_cba["Country"])
        df_eora_cba = df_eora_cba[~df_eora_cba["country"].contains("NOT FOUND")]




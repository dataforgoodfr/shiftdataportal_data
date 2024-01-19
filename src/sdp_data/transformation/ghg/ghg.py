import pandas as pd
from sdp_data.utils.translation import (
    CountryTranslatorFrenchToEnglish,
    SectorTranslator,
)
from sdp_data.utils.iso3166 import countries_by_name
from sdp_data.transformation.ghg.edgar import EdgarCleaner
from sdp_data.transformation.demographic.countries import (
    StatisticsPerCountriesAndZonesJoiner,
)
import numpy as np
from sdp_data.utils.format import StatisticsDataframeFormatter


class CombinatorEdgarAndUnfcccAnnexes:
    def run(self, df_edgar_clean, df_unfccc_annex_clean, df_country):
        # stack Unfccc and Edgar data
        df_unfccc_annex_clean["source"] = "unfccc"
        df_edgar_clean["source"] = "edgar"
        df_edgar_unfccc_stacked = pd.concat(
            [df_edgar_clean, df_unfccc_annex_clean], axis=0
        )

        # merge with countries
        list_group_by = [
            "group_type",
            "group_name",
            "source",
            "year",
            "sector",
            "gas",
            "ghg_unit",
        ]
        dict_aggregation = {"ghg": "sum"}
        df_edgar_unfccc_stacked_per_zones_and_countries = (
            StatisticsPerCountriesAndZonesJoiner().run(
                df_edgar_unfccc_stacked, df_country, list_group_by, dict_aggregation
            )
        )

        # aggregate per gas and per sector
        groupby_by_gas = ["source", "group_type", "group_name", "year", "gas"]
        df_ghg_edunf_by_gas = (
            df_edgar_unfccc_stacked_per_zones_and_countries.groupby(groupby_by_gas)
            .agg(ghg=("ghg", "sum"), ghg_unit=("ghg_unit", "first"))
            .reset_index()
        )

        groupby_by_sector = ["source", "group_type", "group_name", "year", "sector"]
        df_ghg_edunf_by_sector = (
            df_edgar_unfccc_stacked_per_zones_and_countries.groupby(groupby_by_sector)
            .agg(ghg=("ghg", "sum"), ghg_unit=("ghg_unit", "first"))
            .reset_index()
        )

        return df_ghg_edunf_by_gas, df_ghg_edunf_by_sector


class GhgPikEdgarCombinator:

    @staticmethod
    def compute_pik_edgr_stacked(df_pik_clean, df_edgar_clean):
        df_pik_edgar_stacked = pd.concat([df_pik_clean, df_edgar_clean], axis=0)
        df_pik_edgar_stacked = StatisticsDataframeFormatter().select_and_sort_values(df_pik_edgar_stacked, "ghg", round_statistics=4)
        return df_pik_edgar_stacked

    def compute_pik_edgar(self, df_pik_clean, df_edgar_clean):
        # filter Edgat on relevant sectors
        df_edgar_filter_sector = df_edgar_clean[
            df_edgar_clean["sector"].isin(
                "Transport", "Electricity & Heat", "Other Energy"
            )
        ]

        # Select rows from with sector = 'Industry and Construction' and merge
        df_edgar_industry = df_edgar_clean[
            df_edgar_clean["sector"] == "Industry and Construction"
        ]
        df_pik_industry = df_pik_clean[
            df_pik_clean["sector"] == "Industry and Construction"
        ]
        df_union_industry = pd.merge(
            df_edgar_industry,
            df_pik_industry,
            on=["country", "year", "gas"],
            suffixes=("_edgar", "_pik"),
        )
        df_union_industry["ghg"] = (
            df_union_industry["ghg_edgar"] - df_union_industry["ghg_pik"]
        )
        df_union_industry = df_union_industry[
            ["country", "sector", "gas", "year", "ghg", "ghg_unit"]
        ]

        # finally Concatenate df_c1 and df_union
        df_result = pd.concat([df_edgar_filter_sector, df_union_industry])
        return df_result

    def run(self, df_edgar_clean, df_unfccc_annex_clean, df_pik_clean, df_country):
        """ """
        # merge PIK data and UNFCC data and Edgar data
        df_pik_unfccc = pd.concat([df_pik_clean, df_unfccc_annex_clean], axis=0)

        # merge PIK data and Edgar data
        df_pik_edgar_stacked = pd.concat([df_pik_unfccc, df_edgar_clean], axis=0)
        df_pik_edgar_merge = self.compute_pik_edgar(df_pik_clean, df_edgar_clean)

        # stack Unfccc and Edgar data and merge with countries
        df_pik_unfccc["source"] = "unfccc"
        df_edgar_clean["source"] = "edgar_cleaned2"
        df_pik_edgar_stacked = pd.concat([df_pik_unfccc, df_edgar_clean], axis=0)
        list_group_by = [
            "group_type",
            "group_name",
            "source",
            "year",
            "sector",
            "gas",
            "ghg_unit",
        ]
        dict_aggregation = {"ghg": "sum"}
        df_pik_edgar_stacked_per_zones_and_countries = (
            StatisticsPerCountriesAndZonesJoiner().run(
                df_pik_edgar_stacked, df_country, list_group_by, dict_aggregation
            )
        )

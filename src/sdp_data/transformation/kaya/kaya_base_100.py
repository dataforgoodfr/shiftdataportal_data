from typing import Union, List

import pandas as pd


class KayaBase100Processor:

    def run(
            self,
            df_population: pd.DataFrame,
            df_consumption_primary_energy: pd.DataFrame,
            df_co2_emissions: pd.DataFrame,
            df_gdp: pd.DataFrame,
    ) -> pd.DataFrame:
        base_table = self._prepare_base_table(
            df_population=df_population,
            df_consumption_primary_energy=df_consumption_primary_energy,
            df_co2_emissions=df_co2_emissions,
            df_gdp=df_gdp,
        )
        kaya_base_100 = self._calculate_yearly_variation_rates(base_table)
        return self._add_base_100_factors(kaya_base_100)


    def _add_base_100_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        factors = [
            "population",
            "co2",
            "gdp_per_capita",
            "energy_per_gdp",
            "co2_per_energy",
        ]
        for factor in factors:
            df = self._add_base_100_factor(df=df, factor=factor)
        return df


    @staticmethod
    def _add_base_100_factor(df: pd.DataFrame, factor: str) -> pd.DataFrame:
        df = df.sort_values(["group_type", "group_name", "gdp_unit", "year"])

        factors_base_100 = []
        factor_base_100_y_minus_1 = 100
        for _, row in df.iterrows():
            factor_base_100 = factor_base_100_y_minus_1 * (1 + row[factor]) if not pd.isna(row[f"{factor}_y_minus_1"]) else 100
            factor_base_100_y_minus_1 = factor_base_100
            factors_base_100.append(factor_base_100)

        df[f"{factor}_b100"] = factors_base_100
        return df


    def _calculate_yearly_variation_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        kaya_columns = [
            "population",
            "energy",
            "co2",
            "gdp",
            "gdp_per_capita",
            "energy_per_gdp",
            "co2_per_energy",
        ]
        df = self._add_y_minus_1_columns(df=df, columns=kaya_columns)
        df = self._calculate_variation_rates(df=df, columns=kaya_columns)
        return df


    @staticmethod
    def _calculate_variation_rates(df: pd.DataFrame, columns: Union[str, List[str]]) -> pd.DataFrame:
        if isinstance(columns, str):
            columns = [columns]

        for colname in columns:
            df[f"{colname}_lag_diff"] = df[colname]-df[f"{colname}_y_minus_1"]
            # This is the right calculation of the variation rate but we use
            # the wrong one to be able to compare the Dataiku dataset and ours.
            # df[f"{colname}_variation"] = df[f"{colname}_lag_diff"]/df[f"{colname}_y_minus_1"]
            df[f"{colname}_variation"] = df[f"{colname}_lag_diff"]/df[colname]

        return df


    @staticmethod
    def _add_y_minus_1_columns(df: pd.DataFrame, columns: Union[str, List[str]]) -> pd.DataFrame:
        if isinstance(columns, str):
            columns = [columns]

        country_groups = df.groupby(
            ["group_type", "group_name", "gdp_unit"]
        ).apply(lambda group: group.sort_values("year"))
        for colname in columns:
            country_groups[f"{colname}_y_minus_1"] = country_groups[colname].shift(1)

        return country_groups.reset_index(drop=True)


    @staticmethod
    def _prepare_base_table(
            df_population: pd.DataFrame,
            df_consumption_primary_energy: pd.DataFrame,
            df_co2_emissions: pd.DataFrame,
            df_gdp: pd.DataFrame,
    ) -> pd.DataFrame:
        df = df_population.merge(
            df_consumption_primary_energy,
            on=["group_type", "group_name", "year"],
        ).merge(
            df_co2_emissions,
            on=["group_type", "group_name", "year"],
            # The suffixes will help to distinguish the source columns
            suffixes=("_primary_energy", "_co2_emissions"),
        ).merge(
            df_gdp,
            on=["group_type", "group_name", "year"],
        )
        df["gdp_per_capita"] = df.gdp/df.population
        df["energy_per_gdp"] = df.energy/df.gdp
        df["co2_per_energy"] = df.co2/df.energy

        # We keep type and source_primary_energy columns as they are in Dataiku
        # but these 2 columns don't seem to be used anywhere else.
        return df[[
            "group_type",
            "group_name",
            "year",
            "population",
            "type",
            "source_primary_energy",
            "energy",
            "energy_unit",
            "co2",
            "co2_unit",
            "gdp",
            "gdp_unit",
            "gdp_per_capita",
            "energy_per_gdp",
            "co2_per_energy",
        ]]

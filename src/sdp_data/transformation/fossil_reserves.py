import pandas as pd
import numpy as np
import sys
sys.path.insert(0, r'C:\Users\HP\Desktop\shiftdataportal_data')
from src.sdp_data.utils.translation import CountryTranslatorFrenchToEnglish


class coal_reserves_consolidated_prod_Genertor:

    def __init__(self):
        # Initialize any necessary variables or state here
        pass

    def process(self, row):
        row["year"] = int(row["year"])
        if not pd.isna(row["proven_reserves"]) and isinstance(row["proven_reserves"], str):
            row["proven_reserves"] = row["proven_reserves"].replace(" ", "").replace("-", "")
        try:
            row["proven_reserves"] = int(row["proven_reserves"])
        except ValueError:
            # Handle cases where conversion to int fails
            row["proven_reserves"] = np.nan

        if row["coal_type"] == "Hard Coal":
            # 1.5 tons of hard coal = 1 ton of
            row["proven_reserves_energy"] = row["proven_reserves"] / 1.5
        elif row["coal_type"] == "Brown Coal":
            # 3 tons of brown coal = 1 ton of oil
            row["proven_reserves_energy"] = row["proven_reserves"] / 3
        else:
            row["proven_reserves_energy"] = None

        row["proven_reserves_energy_unit"] = "Mtoe"
        return row

    def run(self, df_coal_reserves: pd.DataFrame) -> pd.DataFrame:
        """
        Computes Coal proven reserves for each country, zone.
        """
        df_coal_reserves = pd.melt(df_coal_reserves, id_vars=["country", "unit", "coal_type"], var_name='year',
                                   value_name='proven_reserves')

        df_coal_reserves = df_coal_reserves.apply(self.process, axis=1)
        df_coal_reserves.rename(columns={'country': 'group_name'}, inplace=True)
        df_coal_reserves['source'] = "Survey of Energy Resources, World Energy Council 2010"
        df_coal_reserves_grouped = df_coal_reserves.groupby(
            ['group_name', 'year', 'proven_reserves_energy_unit', 'source'])
        df_coal_reserves_consoliated = df_coal_reserves_grouped['proven_reserves_energy'].sum().reset_index()
        # Renaming the summed column to 'proven_reserves'
        df_coal_reserves_consoliated.rename(columns={'proven_reserves_energy': 'proven_reserves'}, inplace=True)

        return df_coal_reserves_consoliated


class oil_proven_reserves_normalized_Generator:

    def __init__(self):
        pass

    def run(self, df_oil_proven_reserves: pd.DataFrame) -> pd.DataFrame:
        """
        Computes the translation of countries names of the data of oil proven reserves.
        """
        df_oil_proven_reserves["country"] = CountryTranslatorFrenchToEnglish().run(df_oil_proven_reserves["country"],
                                                                                   raise_errors=False)
        df_oil_proven_reserves = df_oil_proven_reserves[df_oil_proven_reserves["country"] != "Delete"]

        return df_oil_proven_reserves


class oil_proven_reserves_prod_Genertor:

    def __init__(self):
        # Initialize any necessary variables or state here
        pass

    def run(self, df_country: pd.DataFrame, df_oil_proven_reserves_normalized: pd.DataFrame) -> pd.DataFrame:
        """
        Computes oil proven reserves for each country, zone.
        """
        # Perform a left join between the groups and reserves dataframes on the 'country' column
        merged_df = pd.merge(df_country, df_oil_proven_reserves_normalized, on='country', how='left')

        # Group by group_type, group_name, and unit, then sum the proven_reserves
        grouped_df = merged_df.groupby(['group_type', 'group_name', 'unit'], as_index=False)['proven_reserves'].sum()

        # For the second part of the union, select the countries directly with their proven reserves and unit
        countries_df = df_oil_proven_reserves_normalized[['country', 'proven_reserves', 'unit']].copy()
        countries_df.rename(columns={'country': 'group_name'}, inplace=True)
        countries_df['group_type'] = 'country'

        # Concatenate the two dataframes to mimic the UNION ALL operation
        final_df = pd.concat([grouped_df, countries_df], ignore_index=True, sort=False)

        return final_df


class gas_proven_reserves_normalized_Genertor:

    def __init__(self):
        pass

    def run(self, df_gas_proven_reserves: pd.DataFrame) -> pd.DataFrame:
        """
        Computes the translation of countries names of the data of gas proven reserves.
        """
        df_gas_proven_reserves["country"] = CountryTranslatorFrenchToEnglish().run(df_gas_proven_reserves["country"],
                                                                                   raise_errors=False)
        df_gas_proven_reserves = df_gas_proven_reserves[df_gas_proven_reserves["country"] != "Delete"]

        return df_gas_proven_reserves


class bp_fossil_with_zones_prod_Genertor:

    def __init__(self):
        # Initialize any necessary variables or state here
        pass

    def process(self, row):
        row["year"] = int(row["year"])
        if not pd.isna(row["proven_reserves"]) and isinstance(row["proven_reserves"], str):
            row["proven_reserves"] = row["proven_reserves"].replace(" ", "").replace("-", "")
        try:
            row["proven_reserves"] = float(row["proven_reserves"])
        except ValueError:
            # Handle cases where conversion to int fails
            row["proven_reserves"] = np.nan

        row["proven_reserves_unit"] = "Bcm" if row['energy_source'] == 'Gas' else 'Gb'
        return row

    def run(self, df_bp_gas_proven_reserves: pd.DataFrame, df_bp_oil_proven_reserves: pd.DataFrame, df_country: pd.DataFrame) -> pd.DataFrame:
        """
        Computes fossil (gas,oil) proven reserves for each country, zone for the source BP.
        """
        # Add 'energy_source' column to each DataFrame
        df_bp_gas_proven_reserves['energy_source'] = 'Gas'
        df_bp_oil_proven_reserves['energy_source'] = 'Oil'

        # Combines the gas and oil DataFrames.
        df_bp_oilgal_proven_reserves_stacked = pd.concat([df_bp_gas_proven_reserves, df_bp_oil_proven_reserves])

        # Transforms the combined DataFrame so that each year's data becomes a separate row.
        df_bp_oilgal_proven_reserves_stacked = pd.melt(df_bp_oilgal_proven_reserves_stacked,
                                                       id_vars=["country", "energy_source"], var_name='year',
                                                       value_name='proven_reserves')

        # Applies the process method to clean and convert proven reserves data.
        df_bp_oilgal_proven_reserves_stacked = df_bp_oilgal_proven_reserves_stacked.apply(self.process, axis=1)

        df_bp_oilgal_proven_reserves_stacked.dropna(subset='proven_reserves', inplace=True)

        df_bp_oilgal_proven_reserves_stacked = df_bp_oilgal_proven_reserves_stacked.reset_index(drop=True)

        # Apply translation to countries names from frensh to english
        df_bp_oilgal_proven_reserves_stacked["country"] = CountryTranslatorFrenchToEnglish().run(
            df_bp_oilgal_proven_reserves_stacked["country"], raise_errors=False)
        df_bp_oilgal_proven_reserves_stacked = df_bp_oilgal_proven_reserves_stacked[
            df_bp_oilgal_proven_reserves_stacked["country"] != "Delete"]

        # Perform a left join between the two DataFrames on the 'country' column
        merged_df = pd.merge(df_country, df_bp_oilgal_proven_reserves_stacked, on='country', how='left')

        # Group by the necessary columns and calculate the sum of proven reserves
        grouped_df = merged_df.groupby(['group_type', 'group_name', 'energy_source', 'year', 'proven_reserves_unit'],
                                       as_index=False)['proven_reserves'].sum()

        # Filter out groups where the sum of proven reserves is not null
        grouped_df = grouped_df[grouped_df['proven_reserves'].notnull()].reset_index(drop=True)

        # For the second part of the union, select the rows directly with their original columns
        countries_df = df_bp_oilgal_proven_reserves_stacked[
            ['country', 'energy_source', 'year', 'proven_reserves', 'proven_reserves_unit']].copy()
        countries_df.rename(columns={'country': 'group_name'}, inplace=True)
        countries_df['group_type'] = 'country'

        # Concatenate the two DataFrames to mimic the UNION ALL operation
        final_df = pd.concat([grouped_df, countries_df], ignore_index=True, sort=False)
        # diference there is no other cis and other europpe, in translatio n I added them+6

        return final_df


class fossil_proven_reserves_prod_Genertor:

    def __init__(self):
        # Initialize any necessary variables or state here
        pass

    def process(self, row):
        row["year"] = int(row["year"])
        if not pd.isna(row["proven_reserves"]) and isinstance(row["proven_reserves"], str):
            row["proven_reserves"] = row["proven_reserves"].replace(" ", "").replace("-", "")
        try:
            row["proven_reserves"] = float(row["proven_reserves"])
        except ValueError:
            # Handle cases where conversion to int fails
            row["proven_reserves"] = np.nan

        row["proven_reserves_unit"] = "Bcm" if row['energy_source'] == 'Gas' else 'Gb'
        return row

    def run(self, df_gas_proven_reserves: pd.DataFrame, df_oil_proven_reserves: pd.DataFrame,
                                            df_coal_proven_reserves: pd.DataFrame, df_country: pd.DataFrame) -> pd.DataFrame:
        """
        Computes fossil (gas,oil,coal) proven reserves for each country, zone.
        """
        # Add 'energy_source' column to each DataFrame
        df_gas_proven_reserves['energy_source'] = 'Gas'
        df_oil_proven_reserves['energy_source'] = 'Oil'
        df_coal_proven_reserves['energy_source'] = 'Coal'

        # Combine all fossil fuel data
        df_fossil_proven_reserves_stacked = pd.concat(
            [df_gas_proven_reserves, df_oil_proven_reserves, df_coal_proven_reserves])
        df_fossil_proven_reserves_stacked['country'] = df_fossil_proven_reserves_stacked['country'].str.replace('\xa0',
                                                                                                                ' ').str.strip()
        df_fossil_proven_reserves_stacked["country"] = CountryTranslatorFrenchToEnglish().run(
            df_fossil_proven_reserves_stacked["country"], raise_errors=False)

        # Remove rows with NaN in 'country'
        df_fossil_proven_reserves_stacked.dropna(subset='country', inplace=True)

        # Perform a left join between the two DataFrames on the 'country' column
        merged_df = pd.merge(df_country, df_fossil_proven_reserves_stacked, on='country', how='left')

        # Group by the necessary columns and calculate the sum of proven reserves
        grouped_df = merged_df.groupby(['group_type', 'group_name', 'energy_source', 'unit'], as_index=False)[
            'proven_reserves'].sum()

        # Filter out groups where the sum of proven reserves is not null
        grouped_df = grouped_df[grouped_df['proven_reserves'].notnull()].reset_index(drop=True)

        # For the second part of the union, select the rows directly with their original columns
        countries_df = df_fossil_proven_reserves_stacked[['country', 'energy_source', 'proven_reserves', 'unit']].copy()
        countries_df.rename(columns={'country': 'group_name'}, inplace=True)
        countries_df['group_type'] = 'country'

        # Concatenate the two DataFrames to mimic the UNION ALL operation
        final_df = pd.concat([grouped_df, countries_df], ignore_index=True, sort=False)
        # resut are not equal exactly because in the dataiku we keep nan value, we don't translate South koreaa, and there is missing value in the result like afghanistan in gas
        return final_df




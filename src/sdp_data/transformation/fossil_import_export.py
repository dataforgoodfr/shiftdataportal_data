import pandas as pd
import numpy as np
import sys

sys.path.insert(0, r'C:\Users\HP\Desktop\shiftdataportal_data')
from src.sdp_data.utils.translation import CountryTranslatorFrenchToEnglish


class FossilProvenReservesProdGenerator:

    def __init__(self):
        # Initialize any necessary variables or state here
        pass

    def process(self, df):
        df["year"] = df["year"].astype(int)
        df["energy"] = df["energy"].str.replace(" ", "").replace("-", "")
        df['energy'] = pd.to_numeric(df['energy'], errors='coerce')
        return df

        return row

    def run(self, df_us_eia_oil, df_us_eia_gas, df_us_eia_coal, df_country):
        df_us_eia_oil = pd.melt(df_us_eia_oil,
                                id_vars=["country", "energy_unit", "energy_source", "type", "subtype", "subsubtype",
                                         "subsubsubtype"], var_name='year', value_name='energy')
        df_us_eia_gas = pd.melt(df_us_eia_gas,
                                id_vars=["country", "energy_unit", "energy_source", "type", "subtype", "subsubtype"],
                                var_name='year', value_name='energy')
        df_us_eia_coal = pd.melt(df_us_eia_coal, id_vars=["country", "energy_unit", "energy_source", "type", "subtype"],
                                 var_name='year', value_name='energy')

        df_us_eia_gas = df_us_eia_gas[df_us_eia_gas.subtype == 'Gross natural gas'].reset_index(drop=True)
        df_us_eia_coal = df_us_eia_coal[df_us_eia_coal.subtype == 'Total primary coal'].reset_index(drop=True)

        df_us_eia_oil.drop(columns=["subtype", "subsubtype", "subsubsubtype"], axis=1, inplace=True)
        df_us_eia_gas.drop(columns=["subtype", "subsubtype"], axis=1, inplace=True)
        df_us_eia_coal.drop(columns=["subtype"], axis=1, inplace=True)

        df_us_eia_oil['energy_source'] = 'Oil'
        df_us_eia_gas['energy_source'] = 'Gas'
        df_us_eia_coal['energy_source'] = 'Coal'

        df_grouped = pd.concat([df_us_eia_oil, df_us_eia_gas, df_us_eia_coal]).reset_index(drop=True)

        df_grouped["energy_unit"].replace("MMTOE", "Mtoe", inplace=True)
        df_grouped["source"] = "US EIA"
        df_grouped = df_grouped[df_grouped.type.isin(['Imports', 'Exports', 'Imports Net'])].reset_index(drop=True)
        df_grouped = self.process(df_grouped)
        df_grouped.dropna(subset='energy', inplace=True)
        df_grouped["country"] = CountryTranslatorFrenchToEnglish().run(df_grouped["country"], raise_errors=False)
        df_grouped = df_grouped[df_grouped["country"] != "Delete"]
        df_grouped.rename(columns={'country': 'group_name'}, inplace=True)
        df_grouped = df_grouped.reset_index(drop=True)
        df_net_imports = df_grouped.copy()
        df_net_imports['energy'] = df_net_imports.apply(
            lambda x: x['energy'] if x['type'] == 'Imports' else -x['energy'], axis=1)
        df_net_imports = df_net_imports[df_net_imports['type'].notnull()]
        df_net_imports = df_net_imports.groupby(['group_name', 'energy_unit', 'energy_source', 'year', 'source'])[
            'energy'].sum().reset_index()
        df_net_imports['type'] = 'Net Imports'
        df_grouped = df_grouped[df_grouped['type'].notnull()]
        df_final = pd.concat([df_net_imports, df_grouped])

        return df_final

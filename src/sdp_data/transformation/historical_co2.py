import pandas as pd

class HistoricalCo2PerZoneAndCountryProcessor:

    def __init__(self) -> None:
        self.list_urss_countries = ["Ukraine", "Armenia", "Azerbaijan", "Belarus",
                                    "Estonia", "Georgia", "Kazakhstan", "Kyrgyztan",
                                    "Uzbekistan", "Moldova", "Turkmenistan", "Kyrgyzstan",
                                    "Tajikistan", "Estonia", "Latvia", "Lithuania"]

    def retrieve_urss_countries(self, df_pik_cleaned):
        """
        We have to aggregate all soviet state into the USSR
        """
        condition_urss = (df_pik_cleaned['country'].isin(self.list_urss_countries) and df_pik_cleaned['year'] >= 1922 and df_pik_cleaned['year'] < 1992)
        df_pik_cleaned.loc[condition_urss, 'country'] = 'Russian Federation & USSR'
        return df_pik_cleaned
    
    @staticmethod
    def melt_years(df: pd.DataFrame):
        return pd.melt(df, id_vars=["type", "country", "unit", "gdp_unit"],
                       var_name="year",
                       value_name="co2")

    def run(self, df_pik_cleaned: pd.DataFrame, df_eia_raw: pd.DataFrame, df_country: pd.DataFrame):
        
        # retrieve the historical emission from the PIK dataset
        df_pik_energy = df_pik_cleaned[(df_pik_cleaned['gas'] == 'CO2') & (df_pik_cleaned['sector'] == 'Energy') & (df_pik_cleaned['year'] < 1980)]
        df_pik_energy = df_pik_energy.groupby(['country', 'source', 'year', 'ghg_unit']).agg({'ghg': 'sum'}).reset_index()
        df_pik_energy['co2'] = df_pik_energy['ghg']
        df_pik_energy['co2_unit'] = 'MtCO2'
        df_pik_energy['energy_family'] = 'All Fossil Fuels'

        # correct URSS countries
        df_pik_energy = self.retrieve_urss_countries(df_pik_energy)

        # clean EAI data
        df_eia_raw = self.melt_years(df_eia_raw)
        df_eia_raw = df_eia_raw.rename({'unit': 'co2_unit', 'type': 'energy_family'}, axis=1)
        df_eia_raw["co2_unit"] = df_eia_raw["co2_unit"].replace({"MMTons CO2": "MtCO2"})
        df_eia_raw["co2_unit"] = df_eia_raw["co2_unit"].replace({"Coal and Coke": "Coal", "Petroleum and Other Liquids": "Oil", "Natural Gas": "Gas"})
        df_eia_raw = df_eia_raw[df_eia_raw["energy_family"] != "co2 emissions"]
        df_eia_raw["source"] = "US EIA"
        df_eia_raw["co2"] = pd.to_numeric(df_eia_raw["co2"], errors="coerce")
        df_eia_raw = df_eia_raw.dropna(subset=["co2"])

        # join PIK data, EAI data and countries
        df_us_eia_pik = pd.concat([df_pik_energy, df_eia_raw], axis=0)
        df_us_eia_pik_per_zone = (pd.merge(df_country, df_us_eia_pik, how='left', left_on='country', right_on='country')
                                    .groupby(['group_type', 'group_name', 'year', 'energy_family', 'co2_unit', 'source'])
                                    .agg({'co2': 'sum'})
                                    .reset_index()
                           )
        
        df_us_eia_per_country = df_us_eia_per_country.copy()
        df_us_eia_per_country = df_us_eia_per_country.rename({"country": "group_name"}, axis=1)
        df_us_eia_per_country["group_type"] = "country"
        df_us_eia_per_country = df_us_eia_per_country[["group_type", "group_name", "year", "energy_family", "co2", "co2_unit", "source"]]

        # concatenate countries and zones
        df_us_eia_per_zone_and_countries = pd.concat([df_us_eia_pik_per_zone, df_us_eia_per_country], axis=0)
        df_eia_with_zones_aggregated = (df_us_eia_per_zone_and_countries
                                        .groupby(["group_type", "group_name", "year", "source"])
                                        .agg(ghg=('co2', 'sum'), ghg_unit=("co2_unit", "first"))
                                        )
        return df_us_eia_per_zone_and_countries, df_eia_with_zones_aggregated

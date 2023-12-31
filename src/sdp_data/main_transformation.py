from src.sdp_data.transformation.demographic.population import GapMinderPerZoneAndCountryProcessor, PopulationPerZoneAndCountryProcessor
from src.sdp_data.transformation.demographic.population import StatisticsPerCapitaJoiner
from src.sdp_data.transformation.co2_consumption_based_accounting import EoraCo2TradePerZoneAndCountryProcessor
from src.sdp_data.transformation.footprint_vs_territorial import FootprintVsTerrotorialProcessor
import pandas as pd
import os
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "../../results/raw_new_data")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "../../results/new_prod_data")


class TransformationPipeline:

    def run(self):
        """

        :return:
        """
        # Update demographic data
        df_country = pd.read_csv(f"{RAW_DATA_DIR}/country/country_groups.csv")
        df_country = df_country.sort_values(by=["group_type", "group_name", "country"])
        df_country.to_csv(f"{RESULTS_DIR}/COUNTRY_country_groups_prod.csv", index=False)

        # update population data

        # update footprint vs territorial emissions
        df_eora_cba = pd.read_csv(f"{RAW_DATA_DIR}/co2_cba/national.cba.report.1990.2022.txt", sep="\t")
        df_gcb_territorial = pd.read_excel(f"{RAW_DATA_DIR}/co2_cba/National_Fossil_Carbon_Emissions_2023v1.0.xlsx", sheet_name="Territorial Emissions")
        df_gcb_cba = pd.read_excel(f"{RAW_DATA_DIR}/co2_cba/National_Fossil_Carbon_Emissions_2023v1.0.xlsx", sheet_name="Consumption Emissions")
        df_footprint_vs_territorial = FootprintVsTerrotorialProcessor().run(df_gcb_territorial, df_gcb_cba, df_eora_cba, df_country)
        df_footprint_vs_territorial.to_csv(f"{RESULTS_DIR}/CO2_CONSUMPTION_BASED_ACCOUNTING_footprint_vs_territorial_prod.csv", index=False)

        # Compute populations
        """
        df_gapminder = pd.read_excel("../../data/thibaud/gapminder_population_raw_2.xlsx")
        df_population = pd.read_csv(f"../../data/_processed/_processed/processed_population_worldbank.csv")
        df_country = pd.read_excel("../../data/thibaud/country_groups.xlsx")
        df_gapminder_per_zone_and_countries = GapMinderPerZoneAndCountryProcessor().run(df_gapminder, df_country)
        df_population_per_zone_and_countries = PopulationPerZoneAndCountryProcessor().run(df_population, df_country)

        # Compute CO2 consumption based accounting
        df_gcb_territorial = pd.read_excel("../../data/thibaud/co2_consumption_based_accounting/gcb_territorial.xlsx")
        df_gcb_cba = pd.read_excel("../../data/thibaud/co2_consumption_based_accounting/gcb_cba.xlsx")

        df_eora_co2_trade = pd.read_excel("../../data/thibaud/co2_consumption_based_accounting/eora_co2_trade_sectorwise.xlsx")
        df_trade_by_country, df_trade_by_sector = EoraCo2TradePerZoneAndCountryProcessor().run(df_eora_co2_trade, df_country)

        # compute statistics per capita
        df_population_gm_zones = pd.read_excel("../../data/thibaud/per_capita/population_gm_zones_energy.xlsx")
        df_energy = pd.read_excel("../../data/thibaud/per_capita/energies.xlsx")
        df_energy_consumption = pd.read_excel("../../data/thibaud/per_capita/energy_consumption_per_capita/final_cons_full.xlsx")
        df_ghg_by_sector = pd.read_excel("../../data/thibaud/per_capita/ghg_per_capita/ghg_full_by_sector.xlsx")
        df_historical_co2 = pd.read_excel("../../data/thibaud/per_capita/historical_co2_per_capita/eia_with_zones_aggregated.xlsx")

        df_eora_cba_per_capita = StatisticsPerCapitaJoiner().run_eora_cba_per_capita(df_footprint_vs_territorial, df_population)
        df_energy_per_capita = StatisticsPerCapitaJoiner().run_energy_per_capita(df_energy, df_population_gm_zones)
        df_final_energy_per_capita = StatisticsPerCapitaJoiner().run_final_energy_consumption_per_capita(df_energy_consumption, df_population)
        df_ghg_per_capita = StatisticsPerCapitaJoiner().run_ghg_per_capita(df_ghg_by_sector, df_population)
        df_historical_co2_per_capita = StatisticsPerCapitaJoiner().run_historical_emissions_per_capita(df_historical_co2, df_population)
        """


if __name__ == "__main__":
    TransformationPipeline().run()

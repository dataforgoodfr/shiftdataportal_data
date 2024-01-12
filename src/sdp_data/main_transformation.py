from sdp_data.transformation.demographic.population import GapMinderPerZoneAndCountryProcessor, PopulationPerZoneAndCountryProcessor
from sdp_data.transformation.demographic.population import StatisticsPerCapitaJoiner
from sdp_data.transformation.co2_consumption_based_accounting import EoraCo2TradePerZoneAndCountryProcessor
from sdp_data.transformation.footprint_vs_territorial import FootprintVsTerrotorialProcessor
from sdp_data.transformation.demographic.worldbank_scrap import WorldBankScrapper
from sdp_data.transformation.demographic.gdp import GdpMaddissonPerZoneAndCountryProcessor, GdpWorldBankPerZoneAndCountryProcessor
from sdp_data.transformation.iea import EiaConsumptionGasBySectorProcessor, EiaConsumptionOilPerProductProcessor, EiaFinalEnergyConsumptionProcessor, EiaFinalEnergyPerSectorPerEnergyProcessor, EiaElectricityGenerationByEnergyProcessor, EiaConsumptionOilsPerSectorProcessor, EiaFinalEnergyConsumptionPerSectorProcessor
import pandas as pd
import os
import requests
from pandas import json_normalize

RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "../../results/raw_new_data")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "../../results/new_prod_data")


class TransformationPipeline:

    def process_country_data(self):
        # Update demographic data
        df_country = pd.read_csv(f"{RAW_DATA_DIR}/country/country_groups.csv")
        df_country = df_country.sort_values(by=["group_type", "group_name", "country"])
        df_country.to_csv(f"{RESULTS_DIR}/COUNTRY_country_groups_prod.csv", index=False)
        return df_country

    def process_population_data(self, df_country):
        # update population data (World Bank)
        df_population_raw = WorldBankScrapper().run("population")
        df_population = PopulationPerZoneAndCountryProcessor().run(df_population_raw, df_country)
        df_population.to_csv(f"{RESULTS_DIR}/DEMOGRAPHIC_POPULATION_prod.csv", index=False)
        return df_population

    def process_footprint_vs_territorial_data(self, df_country):
        # update footprint vs territorial
        df_eora_cba = pd.read_csv(f"{RAW_DATA_DIR}/co2_cba/national.cba.report.1990.2022.txt", sep="\t")
        df_gcb_territorial = pd.read_excel(f"{RAW_DATA_DIR}/co2_cba/National_Fossil_Carbon_Emissions_2023v1.0.xlsx", sheet_name="Territorial Emissions")
        df_gcb_cba = pd.read_excel(f"{RAW_DATA_DIR}/co2_cba/National_Fossil_Carbon_Emissions_2023v1.0.xlsx", sheet_name="Consumption Emissions")
        df_footprint_vs_territorial = FootprintVsTerrotorialProcessor().run(df_gcb_territorial, df_gcb_cba, df_eora_cba, df_country)
        df_footprint_vs_territorial.to_csv(f"{RESULTS_DIR}/CO2_CONSUMPTION_BASED_ACCOUNTING_footprint_vs_territorial_prod.csv", index=False)

        df_footprint_vs_territorial_per_capita = StatisticsPerCapitaJoiner().run_footprint_vs_territorial_per_capita(df_footprint_vs_territorial, df_population)
        df_footprint_vs_territorial_per_capita.to_csv(f"{RESULTS_DIR}/CO2_CBA_PER_CAPITA_eora_cba_zones_per_capita_prod.csv", index=False)
    
    def process_iea_data(self, df_country):
        
        # gas products
        df_gas_cons_by_sector = EiaConsumptionGasBySectorProcessor().prepare_data(df_country)
        df_gas_cons_by_sector.to_csv(f"{RESULTS_DIR}/FINAL_CONS_GAS_BY_SECTOR_prod.csv", index=False)

        # oild products 
        df_oil_cons_per_product = EiaConsumptionOilPerProductProcessor().prepare_data(df_country)
        df_oil_cons_per_product.to_csv(f"{RESULTS_DIR}/FINAL_CONS_OIL_BY_PRODUCT_prod.csv", index=False)

        df_oil_cons_per_sector = EiaConsumptionOilsPerSectorProcessor().prepare_data(df_country)
        df_oil_cons_per_sector.to_csv(f"{RESULTS_DIR}/FINAL_CONS_OIL_BY_SECTOR_prod.csv", index=False)

        # final energy
        df_final_energy_consumption = EiaFinalEnergyConsumptionProcessor().prepare_data(df_country)
        df_final_energy_consumption.to_csv(f"{RESULTS_DIR}/FINAL_ENERGY_CONSUMPTION_prod.csv", index=False)

        df_final_energy_consumption_per_sector = EiaFinalEnergyConsumptionPerSectorProcessor().prepare_data(df_country)
        df_final_energy_consumption_per_sector.to_csv(f"{RESULTS_DIR}/FINAL_ENERGY_CONSUMPTION_PER_SECTOR_prod.csv", index=False)

        df_energy_per_sector_per_energy_family = EiaFinalEnergyPerSectorPerEnergyProcessor().prepare_data(df_country)
        df_energy_per_sector_per_energy_family.to_csv(f"{RESULTS_DIR}/FINAL_ENERGY_PER_SECTOR_PER_ENERGY_FAMILY_prod.csv", index=False)

        # electricity generation
        electricity_generator = EiaElectricityGenerationByEnergyProcessor().prepare_data(df_country)
        df_electricity_generation = electricity_generator.df_electricity_by_energy_family
        df_electricity_generation.to_csv(f"{RESULTS_DIR}/ELECTRICITY_GENERATION_prod.csv", index=False)

        df_electricity_nuclear_share = electricity_generator.compute_nuclear_share_in_electricity()
        df_electricity_nuclear_share.to_csv(f"{RESULTS_DIR}/ELECTRICITY_NUCLEAR_SHARE_prod.csv", index=False)

        df_electricity_co2_intensity = electricity_generator.compute_co2_intensity_in_electricity()
        df_electricity_co2_intensity.to_csv(f"{RESULTS_DIR}/ELECTRICITY_CO2_INTENSITY_prod.csv", index=False)


    def run(self):
        """

        :return:
        """
        # demographic data
        df_country = self.process_country_data()
        # df_population = self.process_population_data(df_country)

        # consumption-based accounting
        # self.process_footprint_vs_territorial_data(df_country)

        # EAI data
        self.process_iea_data(df_country)


        # update GDP data (World Bank)
        """
        df_gdp_raw = WorldBankScrapper().run("gdp")
        df_population = GdpWorldBankPerZoneAndCountryProcessor().run(df_gdp_raw, df_country)
        df_population.to_csv(f"{RESULTS_DIR}/DEMOGRAPHIC_GDP_prod.csv", index=False)
        """



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

import pandas as pd
from sdp_data.utils.translation import CountryTranslatorFrenchToEnglish
from sdp_data.utils.iso3166 import countries_by_alpha3


class EoraCo2TradePerZoneAndCountryProcessor:

    def __init__(self):
        self.countries_by_alpha3 = countries_by_alpha3

    def translate_country_code_to_country_name(self, serie_country_code_to_translate: pd.Series, raise_errors: bool):
        serie_country_translated = serie_country_code_to_translate.map(self.countries_by_alpha3)
        countries_no_translating = serie_country_translated[serie_country_translated.isnull()].values.tolist()
        print("WARN : no translating found for countries %s" % countries_no_translating)
        if raise_errors and serie_country_code_to_translate.isnll().sum() > 0:
            raise ValueError("ERROR : no translating found for countries %s" % countries_no_translating)

        return serie_country_translated

    @staticmethod
    def unstack_countries_in_dataframe(df: pd.DataFrame):
        df = df.unstack().reset_index()
        df.columns = ["country_to", "ghg", "country_from", "sector"]
        return df

    @staticmethod
    def compute_co2_exchanges_between_countries(df_eora_co2_trade):
        """
        Compute the CO2 exchanges between countries by taking into account both exports and imports.
        :param df_eora_co2_trade: (dataframe) containing the ghg for each pair of countries and sector.
        :return: df_trade_by_country: contains the sum of exchange between each pair of countries.
        """
        # TODO - re-vérifier le calcul des exchanges entre pays.
        # compute imports between countries
        df_exports = (df_eora_co2_trade
                      .groupby(['country_from', 'country_to', 'ghg_unit'])
                      .agg(co2=('ghg', 'sum')).reset_index()
                      )
        df_exports.loc[df_exports["country_from"] == df_exports["country_to"], "co2"] = 0.0
        df_exports = df_exports.rename({"country_from": "country", "ghg_unit": "co2_unit"})
        df_exports["type"] = "CO2 Exports"

        # compute exports between countries
        df_imports = (df_eora_co2_trade
                      .groupby(['country_to', 'country_from', 'ghg_unit'])
                      .agg(co2=('ghg', 'sum')).reset_index()
                      )
        df_imports.loc[df_exports["country_from"] == df_exports["country_to"], "co2"] = 0.0
        df_imports = df_exports.rename({"country_from": "country", "ghg_unit": "co2_unit"})
        df_exports["type"] = "CO2 Imports"

        df_trade_by_country = pd.concat([df_imports, df_exports], axis=0)

        return df_trade_by_country

    @staticmethod
    def add_and_filter_on_continents(df_trade_by_country: pd.DataFrame, df_country: pd.DataFrame):
        """
        For each country, add the zone and filter only on the continents.
        :param df_trade_by_country: (dataframe) containing the CO2 trade between each pair of countries.
        :param df_country: (dataframe) containing all the countries and their respective zones.
        :return: (dataframe) with zone added and filter on continent
        """
        df_zones = df_country[df_country["group_type"] == "zone"]
        df_zones = df_zones[["country", "group_name"]]
        df_trade_by_country = df_trade_by_country.merge(df_zones, how="left", left_on="country_to", right_on="country")
        df_trade_by_country = df_trade_by_country.rename({"group_name": "continent_to"}, axis=1)
        list_continents = ["North America", "Central and South America", "Asia and Oceania", "Europe", "Africa"]
        df_trade_by_country = df_trade_by_country[df_trade_by_country["continent_to"].isin(list_continents)]
        return df_trade_by_country

    def run(self, df_eora_co2_trade: pd.DataFrame, df_country: pd.DataFrame):

        # translate the countries codes into countries names
        df_eora_co2_trade = df_eora_co2_trade.drop("ROW", axis=1)
        df_eora_co2_trade["country"] = self.translate_country_code_to_country_name(df_eora_co2_trade["country"], raise_errors=True)
        df_eora_co2_trade = df_eora_co2_trade.set_index(["country", "sector"])
        df_eora_co2_trade.columns = self.translate_country_code_to_country_name(df_eora_co2_trade.columns, raise_errors=True)

        # unstack the dataframe on countries
        df_eora_co2_trade = self.unstack_countries_in_dataframe(df_eora_co2_trade)
        df_eora_co2_trade["ghg_unit"] = "MtCO2e"
        df_eora_co2_trade = df_eora_co2_trade[['country_from', 'country_to', 'sector', 'ghg', 'ghg_unit']]

        # compute trade of CO2 between countries and filter on continents.
        df_trade_by_country = self.compute_co2_exchanges_between_countries(df_eora_co2_trade)
        df_trade_by_country = self.add_and_filter_on_continents(df_trade_by_country, df_country)

        # compute trade per sector
        df_exports_sector = (df_eora_co2_trade[df_eora_co2_trade["country_from"] != df_eora_co2_trade["country_to"]]
                             .groupby(['country_from', 'sector', 'ghg_unit'])
                             .agg(co2=('ghg', 'sum')).reset_index()
                             )
        df_exports_sector = df_exports_sector.rename({"country_from": "country", "ghg_unit": "co2_unit"})
        df_exports_sector["type"] = "CO2 Exports"

        df_imports_sector = (df_eora_co2_trade[df_eora_co2_trade["country_from"] != df_eora_co2_trade["country_to"]]
                             .groupby(['country_to', 'sector', 'ghg_unit'])
                             .agg(co2=('ghg', 'sum')).reset_index()
                             )
        df_imports_sector = df_imports_sector.rename({"country_to": "country", "ghg_unit": "co2_unit"})
        df_imports_sector["type"] = "CO2 Imports"

        df_territorial_sector = df_imports_sector.copy(deep=True)
        df_territorial_sector["type"] = "Territorial CO2 emissions"

        df_trade_by_sector = pd.concat([df_exports_sector, df_imports_sector, df_territorial_sector], axis=0)
        df_trade_by_sector["group_type"] = "country"

        return df_trade_by_country, df_trade_by_sector


class Co2ConsumptionBasedAccountingProcessor:

    def run(self, df_gcb_territorial: pd.DataFrame, df_gcb_cba: pd.DataFrame,
            df_eora_cba: pd.DataFrame, df_country: pd.DataFrame):

        # compute the EORA CBA per zones and countries
        list_scope_to_filter = ["Territorial Emissions", "CO2 Footprint"]
        df_eora_cba_per_zone_and_countries = EoraCbaPerZoneAndCountryProcessor().run(df_eora_cba, df_country)
        df_eora_cba_per_zone_and_countries = df_eora_cba_per_zone_and_countries[df_eora_cba_per_zone_and_countries.isin(list_scope_to_filter)]

        # compute the GCB per zones and countries
        df_gcb_per_zone_and_countries = GcbPerZoneAndCountryProcessor().run(df_gcb_territorial, df_gcb_cba, df_country)

        # stack the two datasets together
        df_footprint_vs_territorial = pd.concat([df_eora_cba_per_zone_and_countries, df_gcb_per_zone_and_countries], axis=0)

        return df_footprint_vs_territorial





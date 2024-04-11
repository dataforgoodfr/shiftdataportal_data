import pandas as pd

from src.transformations.country_groups import update_country_groups
from src.transformations.multi_selection_country_groups import process_multi_selection_country_groups
from sdp_data.transformation.kaya import KayaBase100Processor

###########################
# Country groups
###########################

df = pd.read_csv(
    "../data/dataiku/raw/country_project/country_groups_new.csv",
    sep=",",
)
country_groups = update_country_groups(df)
country_groups.to_csv(
    "../data/final/country_groups_prod.csv",
    sep=",",
    index=False,
)

###########################
# Multi-selection groups
###########################

df = pd.read_csv(
    "../data/dataiku/raw/country_project/multiselect_groups.csv",
    sep=",",
)
multi_selection_country_groups = process_multi_selection_country_groups(df)
multi_selection_country_groups.to_csv(
    "../data/final/multiselect_groups_prod.csv",
    sep=",",
    index=False,
)

###########################
# Kaya
###########################

df_population = pd.read_csv(
    "../data/dataiku/raw/kaya_project/population_gm_zones.csv",
    sep=",",
)
kaya_base_100 = KayaBase100Processor.run(
    df_population=df_population,
    df_consumption_primary_energy=None,
    df_co2_emissions=None,
    df_gdp=None
)
import pandas as pd

from src.sdp_data.database import truncate_and_insert, connect_to_database


db_connector = connect_to_database(account_type="read-write-access")

fossil_import_export_df = pd.read_csv(
    "../data/final/fossil_import_export_update.csv",
    sep=",",
)
truncate_and_insert(
    db_connector=db_connector,
    df=fossil_import_export_df,
    schema="public",
    table_name="FOSSIL_IMPORT_EXPORT_us_eia_fossil_zones_prod"
)

db_connector.close()

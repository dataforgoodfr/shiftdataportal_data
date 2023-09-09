from src.data_extraction.data_location import WebsiteFile, Website
from src.data_extraction.data_provider import DataProvider

ember_files = [
    WebsiteFile(
        url="https://ember-climate.org/data-catalogue/yearly-electricity-data/",
        source_file_name_pattern=r"full_release_long_format.*\.csv",
        target_file_path="../data/raw/ember/electricity_all_year.csv"
    ),
    WebsiteFile(
        url="https://ember-climate.org/data-catalogue/monthly-electricity-data/",
        source_file_name_pattern=r"full_release_long_format.*\.csv",
        target_file_path="../data/raw/ember/electricity_all_month.csv"
    ),
]

ember_website = Website(
    base_url="https://ember-climate.org/",
    files=ember_files,
)

EMBER = DataProvider(
    name="ember",
    data_locations=[ember_website],
)

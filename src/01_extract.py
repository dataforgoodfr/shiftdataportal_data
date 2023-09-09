from src.data_extraction import EMBER

# data_providers = [EMBER, PIK, BP, WORLDBANK, OWID]
data_providers = [EMBER]

for data_provider in data_providers:
    data_provider.extract_data()

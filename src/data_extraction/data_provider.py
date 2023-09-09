from typing import List

from src.data_extraction.data_location import DataLocation


class DataProvider:
    def __init__(self, name: str, data_locations: List[DataLocation]):
        self.name = name
        self.data_locations = data_locations

    def extract_data(self) -> None:
        for data_location in self.data_locations:
            data_location.extract_data()

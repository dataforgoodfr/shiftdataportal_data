import re
from abc import ABC, abstractmethod
from typing import List

import requests
from bs4 import BeautifulSoup

from src.data_extraction.download import download_file_with_retry


class DataLocation(ABC):
    @abstractmethod
    def extract_data(self):
        pass


class WebsiteFile:
    def __init__(self, url: str, source_file_name_pattern: str, target_file_path: str):
        self.url = url
        self.source_file_name_pattern = source_file_name_pattern
        self.target_file_path = target_file_path

    def download(self, base_url: str):
        scraped_page = requests.get(self.url)
        html_parser = BeautifulSoup(scraped_page.content, "html.parser")
        links = html_parser.find_all(href=re.compile(self.source_file_name_pattern))
        file_url = [link["href"] for link in links][0]
        full_url = base_url + file_url
        print(full_url)
        download_file_with_retry(
            url=full_url,
            file_path=self.target_file_path
        )


class Website(DataLocation):
    def __init__(self, base_url: str, files: List[WebsiteFile]):
        self.base_url = base_url
        self.files = files

    def extract_data(self) -> None:
        for file in self.files:
            file.download(self.base_url)


class Endpoint(DataLocation):
    def __init__(self, name: str, path: str):
        self.name = name
        self.path = path

    def extract_data(self) -> None:
        ...
        pass

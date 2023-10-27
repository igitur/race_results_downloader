from abc import ABC, abstractmethod

import requests
from bs4 import BeautifulSoup
from loguru import logger

PARSER = "html5lib"


class Scraper(ABC):
    @abstractmethod
    def get_results(self):
        pass


def get(url: str) -> BeautifulSoup:
    logger.debug(f"Downloading {url}")
    response = requests.get(url, timeout=30)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, PARSER)
        return soup

    logger.error(f"Failed to download the URL. Status code: {response.status_code}")
    return None


def get_json(url: str):
    logger.debug(f"Downloading {url}")
    response = requests.get(url, timeout=30)

    if response.status_code == 200:
        return response.json()

    logger.error(f"Error: {response.status_code}")
    return None


def post_json(url: str, data: dict):
    logger.debug(f"Downloading {url}")
    response = requests.post(url, json=data, timeout=30)

    if response.status_code == 200:
        return response.json()

    logger.error(f"Error: {response.status_code}")
    return None

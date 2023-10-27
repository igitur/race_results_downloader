import re

from bs4 import BeautifulSoup
from loguru import logger

from scrapers import scraper

DATA_URL_TEMPLATE = "https://live.ultimate.dk/desktop/front/data.php?results_startrecord=1000000&eventid={eventid}&mode=results&distance={distance_id}&category=&language=us"


class BouttimeScraper(scraper.Scraper):
    url: str = None

    def __init__(self, url):
        self.url = url

    def get_results(self):
        soup = scraper.get(self.url)

        # Delete viewstate elements from soup
        viewstate_elements = [
            "__VIEWSTATE",
            "__VIEWSTATEGENERATOR",
            "__EVENTVALIDATION",
        ]
        for viewstate_element in viewstate_elements:
            for viewstate in soup.find_all(id=viewstate_element):
                if viewstate is not None:
                    viewstate.decompose()

        if soup is None:
            logger.error("Failed to download the URL")
            return []

        results = list(_get_results_from_main(soup))

        return results


def _get_results_from_main(soup: BeautifulSoup) -> list:
    race_name = soup.find(id="ContentPlaceHolder1_lblRaceName").text
    distance_name = soup.find(id="ContentPlaceHolder1_lblDistance").text

    results_table = soup.select_one("div.container table")
    if results_table is None:
        logger.error("No results table found")
        return

    rows = results_table.find_all("tr")
    header_row = rows[0]
    headers = dict(
        (index, _propercase_and_remove_spaces(th.text))
        for index, th in enumerate(header_row.find_all("th"))
        if "d-xs-table-cell" not in th.get("class", [])
    )
    for row in rows[1:]:
        # There are some th cells in the table body! They copied the output format from FinishTime!
        cells = [child for child in row.children if child.name in ["td", "th"]]
        result = {"RaceName": race_name, "EventName": distance_name}
        for index, cell in enumerate(cells):
            if index in headers:
                text = cell.text.strip()
                if headers[index] == "Name" and "(" in text:
                    match = re.match(r"(.*)\s\((.*)\)", text)
                    name = match.group(1)
                    license_number = match.group(2)
                    result["Name"] = name
                    result["LicenseNr"] = license_number
                else:
                    result[headers[index]] = text

        yield result


def _propercase_and_remove_spaces(input_string):
    capitalized_string = " ".join(word.capitalize() for word in input_string.split())
    final_string = capitalized_string.replace(" ", "")
    return final_string

from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from loguru import logger

from scrapers import scraper

DATA_URL_TEMPLATE = "https://live.ultimate.dk/desktop/front/data.php?results_startrecord=1000000&eventid={eventid}&mode=results&distance={distance_id}&category=&language=us"


class UltimateDkScraper(scraper.Scraper):
    url: str = None

    def __init__(self, url):
        self.url = url

    def get_results(self):
        url = _fix_main_page_url(self.url)
        if url is None:
            logger.error("Failed to fix the URL")
            return []

        soup = scraper.get(url)
        if soup is None:
            logger.error("Failed to download the URL")
            return []

        results = list(_get_results_from_main(soup, url))

        return results


def _get_results_from_main(soup: BeautifulSoup, base_url) -> list:
    race_name = (
        soup.find(id="main_screen")
        .select_one("table:nth-last-child(3) td:nth-of-type(2)")
        .text
    )

    event_id = parse_qs(urlparse(base_url).query)["eventid"][0]

    distances = list(_get_distances(soup))
    for distance_id, distance_name in distances:
        distance_id = 1 if distance_id is None else distance_id
        distance_name = race_name if distance_name is None else distance_name

        logger.debug(f"Distance: {distance_name}")
        distance_url = DATA_URL_TEMPLATE.format(
            eventid=event_id,
            distance_id=distance_id,
        )
        results = _get_results_from_distance(distance_url)
        for result in results:
            result["RaceName"] = race_name
            result["EventName"] = distance_name
            yield result


def _get_distances(soup: BeautifulSoup) -> list:
    try:
        search_distance = soup.find(id="search_distance")
        if search_distance is not None:
            options = list(search_distance.find_all("option"))
            if len(options) == 0:
                logger.warning("No distances found. Assuming only one distance.")
                yield (None, None)
                return

            for option in options:
                if "value" not in option.attrs or option.attrs["value"] == "":
                    continue

                distance_id = int(option.attrs["value"])
                distance_name = option.text
                yield (distance_id, distance_name)

    except Exception as e:
        logger.error(f"Failed to get events: {e}")


def _get_results_from_distance(distance_url: str) -> list:
    soup = scraper.get(distance_url)
    if soup is None:
        logger.error("Failed to download the URL")
        return

    rows = soup.select_one("table.search_result_table").find_all("tr")
    header_row = rows[0]
    headers = dict(
        (index, _propercase_and_remove_spaces(td.text))
        for index, td in enumerate(header_row.find_all("td"))
    )
    for row in rows[1:]:
        cells = row.select("td")
        result = {}
        for index, cell in enumerate(cells):
            if index in headers:
                result[headers[index]] = cell.text.strip()

        yield result


def _fix_main_page_url(url: str) -> str:
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    query_parameters = parse_qs(parsed_url.query.lower())
    if "eventid" in query_parameters:
        eventid = query_parameters["eventid"][0]
        return urljoin(base_url, f"/desktop/front/?eventid={eventid}")

    return None


def _propercase_and_remove_spaces(input_string):
    capitalized_string = " ".join(word.capitalize() for word in input_string.split())
    final_string = capitalized_string.replace(" ", "")
    return final_string

import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup
from loguru import logger

from scrapers import scraper


class FinishtimeScraper(scraper.Scraper):
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
    race_name = soup.find(id="ctl00_lblRaceName").text
    events = list(_get_events(soup, base_url))
    for event_name, event_url in events:
        event_name = race_name if event_name is None else event_name
        event_url = base_url if event_url is None else event_url

        logger.debug(f"Event: {event_name} - {event_url}")
        results = _get_results_from_event(event_url)
        for result in results:
            result["RaceName"] = race_name
            result["EventName"] = event_name
            yield result


def _get_events(soup: BeautifulSoup, base_url) -> list:
    try:
        event_select = soup.find(id="ctl00_Content_Main_divEvents")
        if event_select is not None:
            lis = list(event_select.find_all("li"))
            if len(lis) == 0:
                logger.warning("No events found. Assuming only one event.")
                yield (None, None)
                return

            for li in lis:
                anchor = li.find("a")
                event_name = anchor.text
                event_url = urljoin(base_url, anchor["href"])
                yield (event_name, event_url)
        else:
            # Sometimes they have a dropdown
            event_select = soup.find(id="ctl00_Content_Main_cbEvent")
            if event_select is not None:
                form_action = soup.find(id="aspnetForm")["action"]
                race_url = urljoin(base_url, form_action)

                for option in event_select.find_all("option"):
                    event_name = option.text
                    event_url = _append_query_parameters(
                        race_url, {"EId": option.attrs["value"]}
                    )
                    yield (event_name, event_url)

    except Exception as e:
        logger.error(f"Failed to get events: {e}")


def _get_results_from_event(event_url: str) -> list:
    soup = scraper.get(event_url)
    number_of_pages = _get_number_of_pages(soup)
    logger.debug(f"Number of pages: {number_of_pages}")
    for page in range(1, number_of_pages + 1):
        page_url = _append_query_parameters(event_url, {"dt": 0, "PageNo": page})
        logger.debug(f"Page URL: {page_url}")
        soup = scraper.get(page_url)
        results = _get_results_from_page(soup)
        for r in results:
            yield r


def _get_results_from_page(soup: BeautifulSoup) -> list:
    rows = soup.find(id="ctl00_Content_Main_divGrid").find_all("tr")
    header_row = rows[0]
    headers = dict(
        (index, _propercase_and_remove_spaces(th.text))
        for index, th in enumerate(header_row.find_all("th"))
        if "d-xs-table-cell" not in th.get("class", [])
    )
    for row in rows[1:]:
        # There are some th cells in the table body!
        cells = [child for child in row.children if child.name in ["td", "th"]]
        result = {}
        for index, cell in enumerate(cells):
            if index in headers:
                result[headers[index]] = cell.text.strip()

        yield result


def _get_number_of_pages(soup: BeautifulSoup) -> int:
    try:
        t = soup.find(id="ctl00_Content_Main_lblTopPager").text
        match = re.match(r".*of (\d+).*", t)
        if match is not None:
            return int(match.group(1))

        t = soup.find(id="ctl00_Content_Main_grdTopPager").find_all("td")[-1].text
        match = re.match(r"(\d+)", t)
        if match is not None:
            return int(match.group(1))

        return 0
    except Exception as e:
        logger.error(f"Failed to get number of pages: {e}")
        return 0


def _fix_main_page_url(url: str) -> str:
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
    query_parameters = parse_qs(parsed_url.query)
    if "CId" in query_parameters and "RId" in query_parameters:
        cid = query_parameters["CId"][0]
        rid = query_parameters["RId"][0]
        return urljoin(base_url, f"?CId={cid}&RId={rid}")

    return None


def _append_query_parameters(existing_url, additional_params):
    parsed_url = urlparse(existing_url)
    existing_params = parse_qs(parsed_url.query)
    existing_params.update(additional_params)
    updated_query = urlencode(existing_params, doseq=True)
    updated_url = parsed_url._replace(query=updated_query).geturl()

    return updated_url


def _propercase_and_remove_spaces(input_string):
    capitalized_string = " ".join(word.capitalize() for word in input_string.split())
    final_string = capitalized_string.replace(" ", "")
    return final_string

import datetime
import random
import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup
from loguru import logger

from scrapers import scraper

guid_pattern = (
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
guid_regex = re.compile(f"^{guid_pattern}$")

MOBIIELITE_API_BASE_URL = "https://live.mobii.com/"
INCLUDE_ALL_FIELDS = False


class MobiiEliteScraper(scraper.Scraper):
    url: str = None
    race_id: str = None

    def __init__(self, url):
        self.url = url

    def get_results(self):
        self.race_id = _get_race_id(self.url)
        url = _fix_main_page_url(self.url, self.race_id)
        if url is None:
            logger.error("Failed to fix the URL")
            return []

        soup = scraper.get(url)
        if soup is None:
            logger.error("Failed to download the URL")
            return []

        results = list(_get_results_from_main(soup, self.race_id))

        return results


def _get_results_from_main(soup: BeautifulSoup, race_id: str) -> list:
    race_name = soup.find("title").text
    display_id = _get_display_id(soup)

    display_configuration = _get_display_configuration(display_id)[0]

    display_configuration["Columns"].append({"JSONField": "cn", "Name": "EventName"})

    results = _get_results_from_results_engine(display_id, race_id)
    parsed = _parse_results(display_configuration["Columns"], results["Results"])

    parsed = sorted(parsed, key=lambda x: (x["EventName"], x["CoursePosition"]))

    for r in parsed:
        r["RaceName"] = race_name
        yield r


def _parse_results(columns: list, results: list) -> list:
    for r in [rec for rec in results if "ia" in rec and rec["ia"]]:
        result = {}

        r["csp"] = r["cp"]
        r["ctp"] = r["gp"]

        for column in columns:
            if "JSONField" in column and column["JSONField"] in r:
                json_field = column["JSONField"]
                value = r[json_field]
                if column["JSONField"] == "gi":
                    field_name = "Club"
                elif "Field" in column:
                    field_name = column["Field"]
                elif "Name" in column:
                    field_name = column["Name"]
                else:
                    field_name = json_field

                if json_field == "t" or json_field == "p":
                    value = datetime.timedelta(seconds=value / 1000)
                elif json_field == "sti":
                    value = datetime.datetime.fromtimestamp(value / 1000.0)

                result[field_name] = value

            column["is_handled"] = True

        if INCLUDE_ALL_FIELDS:
            for key, value in r.items():
                if key.endswith("Key") or key.endswith("id"):
                    continue

                column = next((c for c in columns if c["JSONField"] == key), None)

                if column is None:
                    result[key] = value
                elif "is_handled" not in column or not column["is_handled"]:
                    if "Field" in column:
                        column_name = column["Field"]
                    elif "Name" in column:
                        column_name = column["Name"]
                    else:
                        column_name = key

                    result[column_name] = value

        yield result


def _time_from_ticks(ticks: int) -> str:
    if ticks is None:
        return None
    return datetime.datetime.fromtimestamp(
        ticks / 1e6
    )  # Divide by 1e6 to convert microseconds to seconds


def _get_courses(race_id) -> list:
    url = urljoin(MOBIIELITE_API_BASE_URL, f"api/Results/GetCourses?RaceId={race_id}")
    return scraper.get_json(url)


def _get_display_id(soup: BeautifulSoup) -> str:
    myTabContent2 = soup.find(id="myTabContent2")
    divs = myTabContent2.select("div[data-src]")
    for div in divs:
        data_src = div.attrs["data-src"]
        if data_src is not None and "DisplayId" in data_src:
            return _get_display_id_from_data_src(data_src)

    logger.error("Could not determine Display ID")
    return None


def _get_display_id_from_data_src(data_src: str) -> str:
    pattern = f"DisplayId=({guid_pattern})"
    match = re.search(pattern, data_src)
    if match is None:
        logger.error("Could not determine Display ID")
        return None
    return match.group(1)


def _get_results_from_results_engine(display_id: str, race_id: str) -> list:
    # url = f"https://live.mobii.com/Result/RenderEngine?DisplayId={display_id}&RaceId={race_id}"

    url = urljoin(MOBIIELITE_API_BASE_URL, "api/Results/GetResults2")

    session_id = _generate_session_id()

    data = {
        "ResultType": 1,
        "CourseEntityType": 4,
        "ModifiedTicks": 0,
        "RaceId": race_id,
        "Index": 0,
        "Count": 1000000,
        "GenderType": 0,
        "GroupItemId": None,
        "SessionId": session_id,
        "Columns": [
            "CourseName",
            "CoursePosition",
            "CategoryPosition",
            "CategoryName",
            "BibNumber",
            "FirstName",
            "LastName",
            "Gender",
            "GroupItem",
            "StartTime",
            "ResultTime",
            "OverallDifference",
            "Pace",
            "Speed",
        ],
    }

    response = scraper.post_json(url, data)
    return response


def _get_display_configuration(display_id: str) -> dict:
    url = urljoin(
        MOBIIELITE_API_BASE_URL,
        f"api/DisplayLayouts/GetDisplayLayoutsForDisplay?displayid={display_id}",
    )
    return scraper.get_json(url)


def _generate_session_id():
    return ("0000" + format(int(random.random() * pow(36, 4)), "x")).zfill(4)[-4:]


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


def _fix_main_page_url(url: str, race_id: str) -> str:
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}/results/RaceID/{race_id}"


def _get_race_id(path: str) -> str:
    pattern = f"/RaceID/({guid_pattern})"
    match = re.search(pattern, path)
    if match is None:
        logger.error("Could not determine Race ID")
        return None

    return match.group(1)


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

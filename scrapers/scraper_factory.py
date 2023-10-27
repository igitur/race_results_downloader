from urllib.parse import urlparse

from scrapers.bouttime_scraper import BouttimeScraper
from scrapers.finishtime_scraper import FinishtimeScraper
from scrapers.mobiielite_scraper import MobiiEliteScraper
from scrapers.scraper import Scraper
from scrapers.ultimate_dk_scraper import UltimateDkScraper


def get_scraper(url: str) -> Scraper:
    """Returns the appropriate scraper for the given URL"""

    parsed = urlparse(url)
    hostname = parsed.hostname.lower()
    if hostname.startswith("www."):
        hostname = hostname[4:]

    if hostname == "results.finishtime.co.za":
        return FinishtimeScraper(url)
    elif hostname == "bouttime.co.za":
        return BouttimeScraper(url)
    elif hostname == "live.ultimate.dk":
        return UltimateDkScraper(url)
    elif hostname == "mobiielite.com":
        return MobiiEliteScraper(url)

    raise ValueError(f"Unknown scraper for URL: {url}")

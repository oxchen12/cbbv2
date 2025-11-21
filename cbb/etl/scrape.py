'''
This module provides functions for scraping data from ESPN.
'''
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any
import json
import logging
import re
import time

from bs4 import BeautifulSoup
# TODO: refactor requests to be async
import requests

logger = logging.getLogger(__name__)

GAME_API_TEMPLATE = (
    'https://site.web.api.espn.com/apis/site/v2/sports/basketball/{}-college-basketball/'
    'summary?region=us&lang=en&contentorigin=espn&event={}'
)
STANDINGS_TEMPLATE = 'https://www.espn.com/{}-college-basketball/standings/_/season/{}'
SCHEDULE_TEMPLATE = 'https://www.espn.com/{}-college-basketball/schedule/_/date/{}'

# request parameters
DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    # TODO: I think this is generic enough that there isn't a security risk
    #       but I should make sure
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}


class League(Enum):
    '''Encodes college basketball leagues.'''
    MENS = 'mens'
    WOMENS = 'womens'


def _get_resp(url: str,
              timeout: int = DEFAULT_TIMEOUT) -> requests.Response:
    # TODO: look into caching this with staleness checks
    # For now, this will not cache anything and will always go out
    # to fetch new information. This should be somewhat okay assuming
    # intelligent usage (i.e., not re-fetching standings and schedule
    # for existing seasons).
    '''Get the raw json from the API.'''
    # TODO: retry handling
    logger.debug('fetching from %s ...', url)
    get_start = time.perf_counter()
    resp = requests.get(
        url=url,
        timeout=timeout,
        headers=DEFAULT_HEADERS
    )
    get_end = time.perf_counter() - get_start
    logger.debug('got %d (%.1fs)', resp.status_code, get_end)
    # TODO: error handling
    if resp.status_code != 200:
        pass

    return resp


def _extract_json(url: str) -> dict[str, Any]:
    '''Extract json data from HTML.'''
    # TODO: error logic
    resp = _get_resp(url)
    soup = BeautifulSoup(resp.text, 'html.parser')
    html_raw = ''
    for x in soup.find_all('script'):
        if str(x).startswith('<script>window'):
            html_raw = str(x).removeprefix(
                '<script>').removesuffix('</script>')
            break

    if html_raw == '':
        # TODO: error logic
        pass

    # TODO: clean up this explanation
    # EXPLANATION
    # - regex split finds assignments for the window object's keys
    # - the second instance of this contains the data we want
    # - remove the residual JS semicolons
    # - load in the cleaned string as json
    # TODO: maybe have a try except block here to handle all the possible
    #       errors in this sqeuence
    json_raw = json.loads(
        re.split(r"window\[.*?\]=", html_raw)[2].replace(';', ''))

    return json_raw


def get_raw_game_json(league: League, gid: int | str) -> dict[str, Any]:
    '''Get the raw json from the game page.'''
    # TODO: error logic
    url = GAME_API_TEMPLATE.format(league.value, gid)
    return _get_resp(url).json()


def get_raw_standings_json(league: League, season: int | str) -> dict[str, Any]:
    '''Get the raw json from the standings page for a season.'''
    # TODO: parameter validation
    url = STANDINGS_TEMPLATE.format(league.value, season)

    return _extract_json(url)


def get_raw_schedule_json(league: League, date: str) -> dict[str, Any]:
    # TODO: accept date as string or dt.date
    '''
    Get the raw json from the schedule page for a given date.
    The date MUST be formatted as YYYYMMDD.
    '''
    # TODO: parameter validation
    url = SCHEDULE_TEMPLATE.format(league.value, date)

    return _extract_json(url)

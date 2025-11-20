'''
This module provides functions for scraping data from ESPN.
'''
from __future__ import annotations

from enum import Enum, auto
from functools import cache
from typing import Any
import json
import re

from bs4 import BeautifulSoup
# TODO: refactor requests to be async
import requests


class Competition(Enum):
    MENS = 'mens'
    WOMENS = 'womens'


class Scraper:
    # url construction
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

    _cache = {}

    def __init__(self,
                 competition: Competition = Competition.MENS):
        self._competition = competition
        self._session = requests.Session()
        self._session.headers.update(Scraper.DEFAULT_HEADERS)

    def _get_resp(self,
                  url: str,
                  timeout: int = DEFAULT_TIMEOUT,
                  headers: dict | None = None) -> requests.Response:
        '''Get the raw json from the API.'''
        # TODO: retry handling
        resp = self._session.get(
            url=url,
            timeout=timeout,
            headers=headers
        )
        # TODO: error handling
        if resp.status_code != 200:
            pass

        return resp

    def _extract_json(self, url: str) -> dict[str, Any]:
        '''Extract json data from HTML.'''
        # TODO: error logic
        resp = self._get_resp(url)
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

    def get_raw_standings_json(self, season: int | str) -> dict[str, Any]:
        '''Get the raw json from the standings page for a season.'''
        # TODO: parameter validation
        url = Scraper.STANDINGS_TEMPLATE.format(
            self._competition.value, season)

        return self._extract_json(url)

    # TODO: accept date as string or dt.date

    def get_raw_schedule_json(self, date: str) -> dict[str, Any]:
        '''
        Get the raw json from the schedule page for a given date.
        The date MUST be formatted as YYYYMMDD.
        '''
        # TODO: parameter validation
        url = Scraper.SCHEDULE_TEMPLATE.format(self._competition.value, date)

        return self._extract_json(url)

'''
This module provides functions for scraping data from ESPN.
'''
from __future__ import annotations

from functools import singledispatch
from typing import Any
import asyncio
import datetime as dt
import json
import logging
import re
import time

from bs4 import BeautifulSoup
import aiohttp
import requests

logger = logging.getLogger(__name__)

# FUTURE: implement switching to women's, perhaps with a module manager
API_PREFIX = (
    'https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball'
)
GAME_API_TEMPLATE = (
    f'{API_PREFIX}/summary?region=us&lang=en&contentorigin=espn&event={{}}'
)
CONFERENCES_API_URL = f'{API_PREFIX}/scoreboard/conferences?groups=50'
WEB_PREFIX = 'https://www.espn.com/mens-college-basketball'
STANDINGS_TEMPLATE = f'{WEB_PREFIX}/standings/_/season/{{}}'
SCHEDULE_TEMPLATE = f'{WEB_PREFIX}/schedule/_/date/{{}}'

# request parameters
DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    # TODO: I think this is generic enough that there isn't a security risk
    #       but I should make sure
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}


def _get_resp(url: str,
              timeout: int = DEFAULT_TIMEOUT) -> requests.Response:
    # TODO: look into caching this with staleness checks
    # For now, this will not cache anything and will always go out
    # to fetch new information. This should be somewhat okay assuming
    # intelligent usage (i.e., not re-fetching standings and schedule
    # for existing seasons).
    '''Get the raw json from the API.'''
    # TODO: retry handling
    logger.debug('fetching from %s...', url)
    get_start = time.perf_counter()
    resp = requests.get(
        url=url,
        timeout=timeout,
        headers=DEFAULT_HEADERS
    )
    get_end = time.perf_counter() - get_start
    logger.debug('got %d (%.2fs)', resp.status_code, get_end)
    # TODO: error handling
    if resp.status_code != 200:
        pass

    return resp


def _extract_json(text: str) -> dict[str, Any]:
    soup = BeautifulSoup(text, 'html.parser')
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


def _extract_json_from_url(url: str) -> dict[str, Any]:
    '''Extract json data from HTML.'''
    # TODO: error logic
    resp = _get_resp(url)
    return _extract_json(resp.text)


def get_game_url(gid: int | str) -> str:
    '''Get the url for the given gid.'''
    return GAME_API_TEMPLATE.format(gid)


def get_standings_url(season: int | str) -> str:
    '''Get the url for the given season's standings.'''
    return STANDINGS_TEMPLATE.format(season)


@singledispatch
def get_schedule_url(date: str):
    '''Get the url for the given date's schedule.'''
    return SCHEDULE_TEMPLATE.format(date)


@get_schedule_url.register
def _(date: dt.date):
    date_str = date.strftime(SCHEDULE_DATE_FORMAT)
    return get_schedule_url(date_str)


def get_raw_game_json(gid: int | str) -> dict[str, Any]:
    '''Get the raw json from the game page.'''
    # TODO: error logic
    url = get_game_url(gid)
    return _get_resp(url).json()


def get_raw_standings_json(season: int | str) -> dict[str, Any]:
    '''Get the raw json from the standings page for a season.'''
    # TODO: parameter validation
    url = get_standings_url(season)
    return _extract_json_from_url(url)


def get_raw_schedule_json(date: str | dt.date) -> dict[str, Any]:
    # TODO: accept date as string or dt.date
    '''
    Get the raw json from the schedule page for a given date.
    The date MUST be formatted as YYYYMMDD.
    '''
    # TODO: parameter validation
    url = get_schedule_url(date)
    return _extract_json_from_url(url)


class AsyncClient:
    '''Provides an interface for async scraping.'''

    def __init__(self):
        self._session = None

    async def __aenter__(self) -> AsyncClient:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers=DEFAULT_HEADERS,
                # TODO: error logic
                raise_for_status=False
            )

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()
            self._session = None

    def check_session_exists(self) -> None:
        '''Asserts whether the session has been initialized.'''
        if self._session is None:
            raise RuntimeError(
                'Client session not initialized. Use `async with`.')

    async def _get_resp(self, url: str) -> aiohttp.ClientResponse:
        # TODO: look into caching this with staleness checks
        # For now, this will not cache anything and will always go out
        # to fetch new information. This should be somewhat okay assuming
        # intelligent usage (i.e., not re-fetching standings and schedule
        # for existing seasons).
        '''Get the raw json from the API.'''
        self.check_session_exists()

        # TODO: retry handling
        logger.debug('fetching from %s...', url)
        get_start = time.perf_counter()
        async with self._session.get(url) as resp:  # type: ignore
            # TODO: error handling
            get_end = time.perf_counter() - get_start
            logger.debug('got %d (%.2fs)', resp.status, get_end)
            if resp.status != 200:
                pass

        return resp

    async def _extract_json(self, url: str) -> dict[str, Any]:
        '''Extract json data from HTML.'''
        self.check_session_exists()

        # TODO: error logic
        async with self._session.get(url) as resp:  # type: ignore
            text = await resp.text(encoding='utf-8')

        return _extract_json(text)

    async def get_raw_game_json(self, gid: int | str) -> dict[str, Any]:
        '''Get the raw json from the game page.'''
        # TODO: error logic
        url = get_game_url(gid)
        resp = asyncio.run(self._get_resp(url))
        return await resp.json()

    async def get_raw_standings_json(self, season: int | str) -> dict[str, Any]:
        '''Get the raw json from the standings page for a season.'''
        # TODO: parameter validation
        url = get_standings_url(season)
        return await self._extract_json(url)

    async def get_raw_schedule_json(self, date: str | dt.date) -> dict[str, Any]:
        '''
        Get the raw json from the schedule page for a given date.
        The date MUST be formatted as SCHEDULE_DATE_FORMAT.
        '''
        # TODO: parameter validation
        url = get_schedule_url(date)
        return await self._extract_json(url)

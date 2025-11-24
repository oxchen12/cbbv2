'''
This module provides functions for scraping data from ESPN.
'''
from __future__ import annotations

from typing import Any
import asyncio
import json
import logging
import re
import time

from bs4 import BeautifulSoup
import aiohttp

logger = logging.getLogger(__name__)

# FUTURE: implement switching to women's, perhaps with a module manager
GAME_API_TEMPLATE = (
    'https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/'
    'summary?region=us&lang=en&contentorigin=espn&event={}'
)
STANDINGS_TEMPLATE = 'https://www.espn.com/mens-college-basketball/standings/_/season/{}'
SCHEDULE_TEMPLATE = 'https://www.espn.com/mens-college-basketball/schedule/_/date/{}'


async def _get_resp(session: aiohttp.ClientSession,
                    url: str) -> aiohttp.ClientResponse:
    # TODO: look into caching this with staleness checks
    # For now, this will not cache anything and will always go out
    # to fetch new information. This should be somewhat okay assuming
    # intelligent usage (i.e., not re-fetching standings and schedule
    # for existing seasons).
    '''Get the raw json from the API.'''
    # TODO: retry handling
    logger.debug('fetching from %s...', url)
    get_start = time.perf_counter()
    # TODO: session should configure its own default timeout and headers
    async with session.get(url) as resp:
        # TODO: error handling
        get_end = time.perf_counter() - get_start
        logger.debug('got %d (%.2fs)', resp.status, get_end)
        if resp.status != 200:
            pass

        return resp


async def _extract_json(session: aiohttp.ClientSession,
                        url: str) -> dict[str, Any]:
    '''Extract json data from HTML.'''
    # TODO: error logic
    resp = asyncio.run(_get_resp(session, url))
    async with resp:
        text = await resp.text(encoding='utf-8')
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


async def get_raw_game_json(session: aiohttp.ClientSession,
                            gid: int | str) -> dict[str, Any]:
    '''Get the raw json from the game page.'''
    # TODO: error logic
    url = GAME_API_TEMPLATE.format(gid)
    resp = asyncio.run(_get_resp(session, url))
    return await resp.json()


async def get_raw_standings_json(session: aiohttp.ClientSession,
                                 season: int | str) -> dict[str, Any]:
    '''Get the raw json from the standings page for a season.'''
    # TODO: parameter validation
    url = STANDINGS_TEMPLATE.format(season)

    return asyncio.run(_extract_json(session, url))


async def get_raw_schedule_json(session: aiohttp.ClientSession,
                                date: str) -> dict[str, Any]:
    # TODO: accept date as string or dt.date
    '''
    Get the raw json from the schedule page for a given date.
    The date MUST be formatted as YYYYMMDD.
    '''
    # TODO: parameter validation
    url = SCHEDULE_TEMPLATE.format(date)

    return asyncio.run(_extract_json(session, url))


class AsyncScrapeClient:
    '''Provides an interface for async scraping.'''

    DEFAULT_TIMEOUT = 30
    DEFAULT_HEADERS = {
        # TODO: I think this is generic enough that there isn't a security risk
        #       but I should make sure
        'User-Agent': 'Mozilla/5.0'
    }

    def __init__(self):
        self._session = None

    async def __aenter__(self) -> AsyncScrapeClient:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers=AsyncScrapeClient.DEFAULT_HEADERS,
                # TODO: error logic
                raise_for_status=False
            )

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()
            self._session = None

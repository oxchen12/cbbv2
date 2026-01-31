"""
This module provides functions for scraping data from ESPN.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import batched
from pathlib import Path
from typing import Any, TypeVar, Callable, Awaitable, Iterable
import asyncio
import datetime as dt
import json
import logging
import re
import time

from bs4 import BeautifulSoup
import aiohttp
import backoff
import duckdb
import polars as pl

from .date import (
    validate_season,
    get_season_start,
    CALENDAR_DT_FORMAT, get_season,
)

logger = logging.getLogger(__name__)
exclude_loggers = ('_log_backoff', '_log_giveup')
for logger_name in exclude_loggers:
    logging.getLogger(logger_name).disabled = True
T = TypeVar('T')
JSONObject = dict[str, Any]

# TODO: implement switching to women's, perhaps with a module manager
# URLs
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
PLAYER_TEMPLATE = f'{WEB_PREFIX}/player/_/id/{{}}'

# request parameters
DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}

# dates
SCHEDULE_DATE_FORMAT = '%Y%m%d'

def is_non_transient(code: int) -> bool:
    return 400 <= code < 500


def _is_giveup_http(e: aiohttp.ClientResponseError) -> bool:
    return is_non_transient(e.status)


def _extract_json(text: str) -> JSONObject:
    soup = BeautifulSoup(text, 'html.parser')
    html_raw = ''
    for x in soup.find_all('script'):
        if str(x).startswith('<script>window'):
            html_raw = str(x).removeprefix(
                '<script>'
            ).removesuffix('</script>')
            break

    if html_raw == '':
        logger.debug('no script tags found, continuing')
        return {}

    # script consists of assignments to properties of `window`
    matches = re.split(r'window\[.*?\]=', html_raw)
    if len(matches) < 3:
        logging.debug('no json data found, continuing')
        return {}
    data_match = matches[2].replace(';', '')
    json_raw = json.loads(data_match)

    return json_raw


def _validate_season(season: int | str):
    try:
        season = int(season)
    except ValueError as e:
        raise ValueError(f'season must be integer-like (got {season})') from e

    validate_season(season)


def get_game_url(game_id: int | str) -> str:
    """Get the url for the given game_id."""
    return GAME_API_TEMPLATE.format(game_id)


def get_standings_url(season: int | str) -> str:
    """Get the url for the given season's standings."""
    return STANDINGS_TEMPLATE.format(season)


def get_schedule_url(date: dt.date):
    """Get the url for the given date's schedule."""
    date_str = date.strftime(SCHEDULE_DATE_FORMAT)
    return SCHEDULE_TEMPLATE.format(date_str)


def get_player_url(player_id: int | str) -> str:
    """Get the url for the given player_id."""
    return PLAYER_TEMPLATE.format(player_id)


class AsyncClient:
    """Provides an interface for async scraping."""

    MIN_MAX_CONCURRENTS = 1
    MAX_MAX_CONCURRENTS = 20
    DEFAULT_MAX_CONCURRENTS = 10

    SLEEP_TIME = 0.1

    def __init__(self, max_concurrents: int = DEFAULT_MAX_CONCURRENTS):
        if (
            max_concurrents < AsyncClient.MIN_MAX_CONCURRENTS
            or max_concurrents > AsyncClient.MAX_MAX_CONCURRENTS
        ):
            raise ValueError(
                f'`max_concurrents` must be between {AsyncClient.MIN_MAX_CONCURRENTS} and {AsyncClient.MAX_MAX_CONCURRENTS}'
            )
        self._semaphore = asyncio.Semaphore(max_concurrents)
        self._session = None

    async def __aenter__(self) -> AsyncClient:
        if self._session is None:
            logger.debug('Creating new session')
            self._session = aiohttp.ClientSession(
                headers=DEFAULT_HEADERS,
                # TODO: error logic
                raise_for_status=True
            )

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            logger.debug('Exiting session')
            await self._session.close()
            self._session = None

    def check_session_exists(self) -> None:
        """Asserts whether the session has been initialized."""
        if self._session is None:
            raise RuntimeError(
                'Client session not initialized. Use `async with`.'
            )

    @backoff.on_exception(
        backoff.expo,
        aiohttp.ClientResponseError,
        giveup=_is_giveup_http,
        raise_on_giveup=True,
        max_tries=5,
        max_time=60,
        factor=2,
        logger=None
    )
    async def _fetch(
        self,
        url: str,
        processor: Callable[[aiohttp.ClientResponse], Awaitable[T]]
    ) -> T:
        """
        Get a request from the url and process the
        response using the `processor`.
        """
        self.check_session_exists()
        short_url = (
            url
            .removeprefix(API_PREFIX)
            .removeprefix(WEB_PREFIX)
        )

        logger.debug('Fetching from %s...', short_url)
        async with self._semaphore:
            get_start = time.perf_counter()
            async with self._session.get(url) as resp:
                get_end = time.perf_counter() - get_start
                logger.debug(
                    'Got %d (%.2fs) from %s',
                    resp.status, get_end, short_url
                )

                await asyncio.sleep(AsyncClient.SLEEP_TIME)
                return await processor(resp)

    async def _extract_json_from_html(self, url: str) -> JSONObject:
        """Extract json from HTML page."""

        async def process_json_from_html(resp: aiohttp.ClientResponse) -> JSONObject:
            text = await resp.text(encoding='utf-8')
            return _extract_json(text)

        return await self._fetch(url, process_json_from_html)

    async def _extract_as_json(self, url: str) -> JSONObject:
        """Extract json from json page."""

        async def process_json(resp: aiohttp.ClientResponse) -> JSONObject:
            return await resp.json()

        return await self._fetch(url, process_json)

    async def get_raw_game_json(
        self,
        game_id: int | str
    ) -> JSONObject:
        """Get the raw json from the game page."""
        url = get_game_url(game_id)
        return await self._extract_as_json(url)

    async def get_raw_standings_json(
        self,
        season: int | str
    ) -> JSONObject:
        """Get the raw json from the standings page for a season."""
        _validate_season(season)
        url = get_standings_url(season)
        return await self._extract_json_from_html(url)

    async def get_raw_schedule_json(
        self,
        date: dt.date
    ) -> JSONObject:
        """
        Get the raw json from the schedule page for a given date.
        A date `str` should be formatted as SCHEDULE_DATE_FORMAT.
        """
        url = get_schedule_url(date)
        return await self._extract_json_from_html(url)

    async def get_raw_player_json(
        self,
        player_id: int | str
    ) -> JSONObject:
        """Get the raw json from the player page."""
        url = get_player_url(player_id)
        return await self._extract_json_from_html(url)

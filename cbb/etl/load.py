'''
This module provides functions for loading transformed
data from ESPN into a local SQLite database.
'''
from typing import Callable
import asyncio
import datetime as dt
import logging

import duckdb
import polars as pl
import polars.selectors as cs

from cbb.etl.database import (
    get_affected_rows
)
from cbb.etl.scrape import (
    get_raw_schedule_json
)
from cbb.etl.transform import (
    transform_from_schedule,
    transform_from_standings,
    transform_from_game,
    transform_from_player
)
from cbb.etl.date import (
    validate_season,
    MIN_SEASON,
    MAX_SEASON,
    DEFAULT_SEASON_START,
    CALENDAR_DT_FORMAT
)

logger = logging.getLogger(__name__)


def _get_rep_dates(
    start: str,
    end: str
) -> list[dt.date]:
    '''
    Get the necessary dates to fetch between start and end from schedules.
    Assumes start and end are formatted like CALENDAR_DT_FORMAT.
    '''
    start_date = dt.datetime.strptime(start, CALENDAR_DT_FORMAT).date()
    start_date = max(
        start_date,
        DEFAULT_SEASON_START.replace(year=start_date.year)
    )
    end_date = dt.datetime.strptime(end, CALENDAR_DT_FORMAT).date()
    calendar = pl.date_range(
        start_date,
        end_date,
        interval=dt.timedelta(days=1),
        eager=True
    )
    # minimize pages to search by accessing schedules from
    # adjacent dates
    rep_dates = [
        date
        for i, date in enumerate(calendar)
        if i % 3 == 1 or i == len(calendar) - 1
    ]

    return rep_dates


def _fix_season_range(
    start_season: int,
    end_season: int
) -> tuple[int, int]:
    if not validate_season(start_season):
        logging.info('Got invalid start_season %d, using default %d',
                     start_season, MIN_SEASON)
        start_season = MIN_SEASON
    if not validate_season(end_season):
        logging.info('Got invalid end_season %d, using default %d',
                     end_season, MAX_SEASON)
        end_season = MAX_SEASON

    if start_season > end_season:
        start_season, end_season = end_season, start_season

    return start_season, end_season


async def _load_season_range(
    conn: duckdb.Connection,
    client: AsyncClient,
    func: Callable,
    start_season: int,
    end_season: int
) -> int:
    '''
    Generic function that collects load results
    from multiple seasons.
    '''
    start_season, end_season = _fix_season_range(start_season, end_season)

    tasks = [
        func(conn, client, season)
        for i in range(start_season, end_season+1)
    ]
    rows = await asyncio.gather(*tasks)

    # TODO: check if I want any more robust error checking.
    #       for now, the system of returning -1 is very simple
    #       but obviously also restrictive

    return get_affected_rows(rows)


async def load_schedule(
    conn: duckdb.Connection,
    client: AsyncClient,
    season: int
) -> int:
    '''
    Load schedule data from the given season into the DB.
    '''
    if not validate_season(season):
        logging.warning('Got invalid season: %d', season)
        return -1

    # use one sync request to get calendar
    season_start = DEFAULT_SEASON_START.replace(year=season)
    init_schedule = get_raw_schedule_json(season_start)
    logger.debug(init_schedule)
    season_json = init_schedule['page']['content']['season']
    # the calendar field for some reason doesn't get every date
    # so instead, we manually generate all dates from start to end
    rep_dates = _get_rep_dates(
        season_json['startDate'], season_json['endDate'])

    tasks = [
        transform_from_schedule(conn, client, rep_date)
        for rep_date in rep_dates
    ]
    rows = await asyncio.gather(*tasks)

    if -1 in rows:
        logger.warning('At least one schedule failed to load properly.')

    return get_affected_rows(rows)


async def load_schedule_range(
    conn: duckdb.Connection,
    client: AsyncClient,
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON
) -> int:
    '''
    Load schedule data from the seasons in a given range.
    '''
    return await _load_season_range(
        conn, client, load_schedule, start_season, end_season
    )


async def load_standings(
    conn: duckdb.Connection,
    client: AsyncClient,
    season: int
) -> int:
    '''
    Load standings data from the given season into the DB.
    '''
    if not validate_season(season):
        logging.warning('Got invalid season: %d', season)
        return -1

    # TODO: not yet implemented
    return -1


async def load_standings_range(
    conn: duckdb.Connection,
    client: AsyncClient,
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON
) -> int:
    return await _load_season_range(
        conn, client, load_standings, start_season, end_season
    )


async def load_all(
    conn: duckdb.Connect,
    client: AsyncClient,
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON
) -> int:
    start_season, end_season = _fix_season_range(start_season, end_season)
    rows = []

    tasks = [
        load_schedule_range(conn, client, start_season, end_season),
        load_standings_range(conn, client, start_season, end_season)
    ]
    rows.extend(await asyncio.gather(*tasks))

    # TODO: get game_ids to update

    # TODO: get player_ids to update

    # TODO: not yet implemented
    return get_affected_rows(rows)

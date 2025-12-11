'''
This module provides functions for loading transformed
data from ESPN into a local SQLite database.
'''
import asyncio
import datetime as dt
import logging

import duckdb
import polars as pl
import polars.selectors as cs


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


async def load_schedule(
    conn: duckdb.Connection,
    client: AsyncClient,
    season: int
) -> int:
    # TODO: consider making this function only grab one day's schedule
    #       this way, the `load` module will be able to control what days
    #       are captured based on existing data in the database
    validate_season(season)

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

    return sum(r for r in rows if r >= 0)

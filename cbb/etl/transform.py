'''
This module provides functions for transforming raw data
from ESPN into pl.DataFrames.
'''
import asyncio
import datetime as dt
import logging

from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
import polars as pl
import polars.selectors as cs

from cbb.etl.scrape import (
    get_raw_game_json,
    get_raw_schedule_json,
    get_raw_standings_json
)

from cbb.etl.async_scrape import Client

geocoder = Nominatim(user_agent='cbb')
tf = TimezoneFinder(in_memory=True)
logger = logging.getLogger(__name__)

DEFAULT_SEASON_START = dt.date(dt.date.today().year, 7, 1)
CALENDAR_DT_FORMAT = '%Y-%m-%dT%H:%MZ'
# TODO: figure out what the hell this is
SEMAPHORE = asyncio.semaphore(50)


def _get_rep_dates(start: str,
                   end: str):
    '''
    Get the necessary dates to fetch between start and end.
    Assumes start and end are formatted like CALENDAR_DT_FORMAT.
    '''
    start = dt.datetime.strptime(start, CALENDAR_DT_FORMAT).date()
    end = dt.datetime.strptime(end, CALENDAR_DT_FORMAT).date()
    duration = (end - start).days
    calendar = [
        (start + dt.timedelta(days=d)).strftime(CALENDAR_DT_FORMAT)
        for d in range(duration)
    ]
    # minimize pages to search by accessing schedules from
    # adjacent dates
    rep_dates = [
        date
        for i, date in enumerate(calendar)
        if i % 3 == 1 or i == len(calendar) - 1
    ]

    return rep_dates


async def extract_from_schedule(season: int) -> pl.DataFrame:
    '''
    Extract data from the schedules for the given season.
    Populates Games, Venues, Statuses.
    '''
    # TODO: parameter validation
    # use one sync request to get calendar
    season_start = DEFAULT_SEASON_START.replace(year=season).strftime('%Y%m%d')
    init_schedule = get_raw_schedule_json(season_start)
    # the calendar field for some reason doesn't get every date
    # so instead, we manually generate all dates from start to end
    season = init_schedule['page']['content']['season']
    rep_dates = _get_rep_dates(season['startDate'], season['endDate'])

    client = Client()
    async with client:
        tasks = [
            client.get_raw_schedule_json(date)
            for date in rep_dates
        ]
        results = await asyncio.run(*tasks)

    results = [
        res['page']['content']['events']
        for res in results
    ]

    # TODO: not yet implemented
    return pl.DataFrame()


def get_plays(gid: int | str) -> pl.DataFrame:
    # TODO: maybe just create a config that defines the default league
    '''
    Get the plays from the raw json and perform cleaning and transformation.
    '''
    # TODO: error logic
    plays_raw = get_raw_game_json(gid)['plays']
    plays = (
        pl.from_dicts(plays_raw)
        .unnest(
            'type',
            'period',
            'clock',
            'coordinate',
            'team',
            separator='_'
        )
        .with_columns(
            pl.col('clock_displayValue').str.split(':')
        )
        .with_columns(
            playerId=(
                pl.col('participants')
                .list.first()
                .struct.field('*')
                .struct.field('*')
                .cast(pl.Int64)
            ),
            assistPlayerId=(
                pl.when(pl.col('participants').list.len().gt(1))
                .then(
                    pl.col('participants')
                    .list.last()
                    .struct.field('*')
                    .struct.field('*')
                    .cast(pl.Int64)
                )
                .otherwise(None)
            ),
            period=pl.col('period_number'),
            periodDisplay=pl.col('period_displayValue'),
            playId=pl.col('id').cast(pl.Int64),
            sequenceId=pl.col('sequenceNumber').cast(pl.Int64),
            playTypeId=pl.col('type_id').cast(pl.Int64),
            playTypeText=pl.col('type_text'),
            teamId=pl.col('team_id').cast(pl.Int64),
            coordinateX=pl.col('coordinate_x'),
            coordinateY=pl.col('coordinate_y'),
            gameClock=pl.duration(
                minutes=pl.col('clock_displayValue').list.get(
                    0).cast(pl.Int64),
                seconds=pl.col('clock_displayValue').list.get(
                    1).cast(pl.Int64),
                time_unit='ms'
            ),
            # TODO: time zone logic from venue
            timestamp=pl.col('wallclock').cast(
                pl.Datetime)  # .dt.convert_time_zone(tz),
        )
        .drop(
            'id', 'participants', 'sequenceNumber', 'wallclock',
            cs.contains('_')
        )
        .select(
            'playId', 'sequenceId', 'text',
            'shortDescription', 'playTypeText', 'playTypeId',
            'shootingPlay', 'pointsAttempted', 'scoringPlay', 'scoreValue',
            'teamId', 'playerId', 'assistPlayerId',
            'period', 'periodDisplay', 'gameClock',
            'awayScore', 'homeScore',
            'coordinateX', 'coordinateY',
            'timestamp'
        )
    )

    return plays

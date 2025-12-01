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
SCHEDULE_KEEP = (
    'id', 'teams', 'date',
    'tbd', 'status', 'venue'
)
# TODO: figure out what the hell this is
SEMAPHORE = asyncio.Semaphore(50)


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


async def transform_from_schedule(season: int) -> pl.DataFrame:
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

    schedule = [
        {
            k: v
            for k, v in event.items()
            if k in SCHEDULE_KEEP
        }
        for res in results
        for date in res['page']['content']['events'].values()
        for event in date
    ]

    games_inter = (
        pl.from_dicts(schedule)
        .lazy()
        .with_columns(
            pl.col('id').cast(pl.Int64),
            pl.col('date').str.to_datetime(time_zone='UTC').alias('datetime'),
            pl.col('teams')
            .list.to_struct(fields=['home', 'away'])
            .struct.unnest()
        )
        .unnest('home', 'away', separator='_')
        .with_columns(
            homeId=pl.col('home_id').cast(pl.Int64),
            awayId=pl.col('away_id').cast(pl.Int64),
        )
        .select(
            cs.exclude(
                'date', 'teams', 'home', 'away',
                cs.starts_with('home_'),
                cs.starts_with('away_')
            )
        )
    )

    venues = (
        games_inter
        .select(pl.col('venue').struct.unnest())
        .unique()
        .with_columns(
            pl.col('id').cast(pl.Int64)
        )
        .sort('id')
        .unnest('address')
        .collect()
    )
    # TODO: insert into Venues

    game_statuses = (
        games_inter
        .select(pl.col('status').struct.unnest())
        .select(
            pl.col('id').cast(pl.Int64),
            'state',
            # remove details with OT specified
            pl.col('detail').str.replace(r'(.+)/\d*OT$', r'$1')
        )
        .unique()
        .sort('id')
        .collect()
    )
    # TODO: insert into GameStatuses

    games = (
        games_inter
        .with_columns(
            venueId=pl.col('venue').struct.field('id').cast(pl.Int64),
            statusId=pl.col('status').struct.field('id').cast(pl.Int64)
        )
        .select(pl.exclude('venue', 'status'))
        .sort('datetime')
        .collect()
    )
    # TODO: insert into Games

    # TODO: not yet implemented
    return pl.DataFrame()


async def transform_from_standings(season: int) -> pl.DataFrame:
    '''
    Extract data from the schedules for the given season.
    Populates Teams, Conferences, ConferenceAlignments.
    '''

    # TODO: not yet implemented
    return pl.DataFrame()


async def transform_from_game(gid: int) -> pl.DataFrame:
    '''
    Extract data from the game.
    Populates Teams, Plays, PlayTypes, Players.
    Updates Games.
    '''
    client = Client()
    async with client:
        game = await client.get_raw_game_json(gid)

    # TODO: not yet implemented
    return pl.DataFrame()


def get_plays(gid: int | str) -> pl.DataFrame:
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
            'shootingPlay', 'pointsAttempted', 'scoringPlay',
            'teamId', 'playerId', 'assistPlayerId',
            'period', 'periodDisplay', 'gameClock',
            'awayScore', 'homeScore',
            'coordinateX', 'coordinateY',
            'timestamp'
        )
    )

    return plays

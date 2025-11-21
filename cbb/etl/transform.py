'''
This module provides functions for transforming raw data
from ESPN into pl.DataFrames.
'''
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
import polars as pl
import polars.selectors as cs

from cbb.etl.scrape import Scraper

geocoder = Nominatim(user_agent='cbb')
tf = TimezoneFinder(in_memory=True)


def get_plays(scraper: Scraper,
              gid: int | str) -> pl.DataFrame:
    '''Get the plays from the raw json and perform cleaning and transformation.'''
    # TODO: error logic
    plays_raw = scraper.get_raw_game_json(gid)['plays']
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

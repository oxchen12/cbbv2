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

from .scrape import (
    get_raw_game_json,
    get_raw_schedule_json,
    get_raw_standings_json,
    AsyncClient
)

from .date import (
    get_season,
    validate_season,
    DEFAULT_SEASON_START
)

logger = logging.getLogger(__name__)

CALENDAR_DT_FORMAT = '%Y-%m-%dT%H:%MZ'
SCHEDULE_KEEP = (
    'id', 'teams', 'date',
    'tbd', 'status', 'venue'
)


def _get_rep_dates(start: str,
                   end: str) -> list[dt.date]:
    '''
    Get the necessary dates to fetch between start and end.
    Assumes start and end are formatted like CALENDAR_DT_FORMAT.
    '''
    start = dt.datetime.strptime(start, CALENDAR_DT_FORMAT).date()
    start = max(start, DEFAULT_SEASON_START)
    end = dt.datetime.strptime(end, CALENDAR_DT_FORMAT).date()
    duration = (end - start).days
    calendar = [
        start + dt.timedelta(days=d)
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


async def transform_from_schedule(client: AsyncClient,
                                  season: int) -> pl.DataFrame:
    '''
    Extract data from the schedules for the given season.
    Populates Games, Venues, Statuses.
    '''
    validate_season(season)

    # use one sync request to get calendar
    season_start = DEFAULT_SEASON_START.replace(year=season)
    init_schedule = get_raw_schedule_json(season_start)
    # the calendar field for some reason doesn't get every date
    # so instead, we manually generate all dates from start to end
    season = init_schedule['page']['content']['season']
    rep_dates = _get_rep_dates(season['startDate'], season['endDate'])
    logging.debug(rep_dates)

    async with client:
        tasks = [
            client.get_raw_schedule_json(date)
            for date in rep_dates
        ]
        schedule_results = await asyncio.gather(*tasks)

    events = [
        {
            k: v
            for k, v in event.items()
            if k in SCHEDULE_KEEP
        }
        for res in schedule_results
        for date in res['page']['content']['events'].values()
        for event in date
    ]

    # TODO: consider moving all of these to external functions
    games_inter = (
        pl.from_dicts(events)
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
    return games


async def transform_from_standings(client: AsyncClient,
                                   season: int) -> pl.DataFrame:
    '''
    Extract data from the schedules for the given season.
    Populates Teams, Conferences, ConferenceAlignments.
    '''
    validate_season(season)

    async with client:
        standings_json_raw = await client.get_raw_standings_json(season)
    standings_content = standings_json_raw['page']['content']

    conferences = (
        pl.from_dicts(standings_content['headerscoreboard']['collegeConfs'])
        .filter(pl.col('name').ne('NCAA Division I'))
        .select(
            pl.col('groupId').cast(pl.Int64).alias('id'),
            'name',
            pl.col('shortName').alias('abbrev')
        )
    )
    # TODO: insert into Conferences

    teams_confs = (
        pl.from_dicts(standings_content['standings']['groups']['groups'])
        .lazy()
        .select(
            pl.col('name').alias('confName'),
            'standings'
        )
        .explode('standings')
        .unnest('standings')
        .unnest('team')
        .join(
            conferences.select(
                pl.col('cid').alias('conference_id'),
                'name'
            ),
            left_on='confName',
            right_on='name'
        )
        .select(
            'conference_id',
            pl.col('id').alias('team_id').cast(pl.Int64),
            'location',
            pl.col('shortDisplayName').alias('mascot'),
            'abbrev'
        )
    )

    teams = (
        teams_confs
        .select(pl.exclude('conference_id'))
        .collect()
    )
    # TODO: insert into Teams

    conference_alignments = (
        teams_confs
        .select(
            'conference_id',
            pl.col('team_id'),
            pl.lit(season).alias('season')
        )
        .collect()
    )
    # TODO: insert into ConferenceAlignments

    # TODO: not yet implemented
    return pl.DataFrame()


def _transform_box(json_raw: dict[str, Any],
                   team_id: int) -> pl.DataFrame:
    return (
        pl.from_dicts(json_raw)
        .lazy()
        .unnest('athlete')
        .with_columns(
            pl.col('shortName')
            .str.extract(r'[A-Za-z]+\. (.+)')
            .alias('last_name'),
        )
        .select(
            pl.col('id').cast(pl.Int64),
            pl.col('displayName')
            .str.strip_suffix(pl.col('last_name'))
            .str.strip_chars(' ')
            .alias('first_name'),
            'last_name',
            pl.col('position')
            .struct.field('abbreviation')
            .alias('position'),
            pl.col('starter').alias('started'),
            ~pl.col('didNotPlay').alias('played'),
            'ejected',
            pl.col('jersey').cast(pl.Int64),
            pl.lit(team_id).alias('team_id')
        )
    )


async def transform_from_game(client: AsyncClient, gid: int) -> pl.DataFrame:
    '''
    Extract data from the game.
    Populates Plays, PlayTypes, Players, PlayerSeasons.
    Updates Teams, Games.
    '''
    async with client:
        game_json_raw = await client.get_raw_game_json(gid)
    attendance = game_json_raw['gameInfo']['attendance']
    competition_raw = game_json_raw['header']['competitions'][0]
    competitors_raw = competition_raw.pop('competitors')
    away_id, home_id = (
        int(x['team']['id'])
        for x in game_json_raw['boxscore']['players']
    )
    away_box_raw, home_box_raw = (
        x['statistics'][0]
        for x in game_json_raw['boxscore']['players']
    )

    teams = (
        pl.from_dicts(competitors_raw)
        .unnest('team', separator='_')
        .unnest('team_groups', separator='_')
        .select(
            pl.col('id').cast(pl.Int64),
            pl.col('team_color').alias('color'),
            pl.col('team_alternateColor').alias('alt_color')
        )
    )
    # TODO: update Teams

    games = (
        pl.from_dicts(competition_raw)
        .select(
            pl.col('id').cast(pl.Int64).alias('game_id'),
            pl.col('date').str.to_date(format='%Y-%m-%dT%H:%MZ'),
            pl.lit(homeId).cast(pl.Int64).alias('home_id'),
            pl.lit(awayId).cast(pl.Int64).alias('away_id'),
            pl.col('neutralSite').alias('is_neutral_site'),
            pl.col('conferenceCompetition').alias('is_conference'),
            pl.col('shotChartAvailable').alias('has_shot_chart'),
            pl.lit(attendance).alias('attendance')
        )
    )
    # TODO: update Games

    plays_inter = (
        pl.from_dicts(game_json_raw['plays'])
        .lazy()
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
            game_id=pl.lit(gid),
            player_id=(
                pl.col('participants')
                .list.first()
                .struct.field('*')
                .struct.field('*')
                .cast(pl.Int64)
            ),
            assist_id=(
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
            period_display=pl.col('period_displayValue'),
            sequence_id=pl.col('sequenceNumber').cast(pl.Int64),
            play_type_id=pl.col('type_id').cast(pl.Int64),
            play_type_text=pl.col('type_text'),
            team_id=pl.col('team_id').cast(pl.Int64),
            x_coord=pl.col('coordinate_x'),
            y_coord=pl.col('coordinate_y'),
            clock_minutes=pl.col(
                'clock_displayValue').list.get(0).cast(pl.Int64),
            clock_seconds=pl.col(
                'clock_displayValue').list.get(1).cast(pl.Int64),
            description=pl.col('shortDescription'),
            away_score=pl.col('awayScore'),
            home_score=pl.col('homeScore'),
            is_shot=pl.col('shootingPlay'),
            is_score=pl.col('scoringPlay'),
            points_attempted=pl.col('pointsAttempted'),
            # TODO: time zone logic from venue
            timestamp=pl.col('wallclock').cast(
                pl.Datetime)  # .dt.convert_time_zone(tz),
        )
        .select(
            'game_id', 'sequence_id', 'text',
            'description', 'play_type_id', 'play_type_text',
            'is_shot', 'points_attempted', 'is_score',
            'team_id', 'player_id', 'assist_id',
            'period', 'period_display',
            'clock_minutes', 'clock_seconds',
            'away_score', 'home_score',
            'x_coord', 'y_coord',
            'timestamp'
        )
    )

    plays = (
        plays_inter
        .select(
            'game_id', 'sequence_id', 'play_type_id',
            'points_attempted', 'is_score',
            'team_id', 'player_id', 'assist_id',
            'period', 'game_clock_minutes', 'game_clock_seconds',
            'home_score', 'away_score', 'x_coord', 'y_coord',
            'timestamp'
        )
        .collect()
    )
    # TODO: insert into Plays

    play_types = (
        plays_inter
        .select(
            pl.col('play_type_id').alias('id'),
            pl.col('play_type_text').alias('description'),
            pl.col('is_shot')
        )
        .collect()
    )
    # TODO: insert into PlayTypes

    players_inter = pl.concat(
        [
            _transform_box(away_json_box, away_id),
            _transform_box(home_json_box, home_id)
        ],
        how='vertical'
    )

    players = (
        players_inter
        .select(
            'id',
            'first_name',
            'last_name',
            'position'
        )
        .collect()
    )
    # TODO: insert into Players

    season = get_season(
        games
        .item(row=0, col='date')
    )
    player_seasons = (
        players_inter
        .select(
            pl.col('id').alias('player_id'),
            'team_id',
            pl.lit(season).alias('season'),
            'jersey'
        )
        .collect()
    )
    # TODO: insert into PlayerSeasons

    game_id = (
        games
        .item(row=0, col='id')
    )
    game_logs = (
        players_inter
        .select(
            pl.col('id').alias('player_id'),
            pl.lit(game_id),
            'played',
            'started',
            'ejected'
        )
        .collect()
    )
    # TODO: insert into GameLogs

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
            game_id=pl.lit(gid),
            player_id=(
                pl.col('participants')
                .list.first()
                .struct.field('*')
                .struct.field('*')
                .cast(pl.Int64)
            ),
            assist_id=(
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
            period_display=pl.col('period_displayValue'),
            sequence_id=pl.col('sequenceNumber').cast(pl.Int64),
            play_type_id=pl.col('type_id').cast(pl.Int64),
            play_type_text=pl.col('type_text'),
            team_id=pl.col('team_id').cast(pl.Int64),
            x_coord=pl.col('coordinate_x'),
            y_coord=pl.col('coordinate_y'),
            clock_minutes=pl.col(
                'clock_displayValue').list.get(0).cast(pl.Int64),
            clock_seconds=pl.col(
                'clock_displayValue').list.get(1).cast(pl.Int64),
            description=pl.col('shortDescription'),
            away_score=pl.col('awayScore'),
            home_score=pl.col('homeScore'),
            is_shot=pl.col('shootingPlay'),
            is_score=pl.col('scoringPlay'),
            points_attempted=pl.col('pointsAttempted'),
            # TODO: time zone logic from venue
            timestamp=pl.col('wallclock').cast(
                pl.Datetime)  # .dt.convert_time_zone(tz),
        )
        .select(
            'game_id', 'sequence_id', 'text',
            'description', 'play_type_id', 'play_type_text',
            'is_shot', 'points_attempted', 'is_score',
            'team_id', 'player_id', 'assist_id',
            'period', 'period_display',
            'clock_minutes', 'clock_seconds',
            'away_score', 'home_score',
            'x_coord', 'y_coord',
            'timestamp'
        )
    )

    return plays

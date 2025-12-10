'''
This module provides functions for transforming raw data
from ESPN into pl.DataFrames.
'''
from typing import Any
import asyncio
import datetime as dt
import logging

import duckdb
import polars as pl
import polars.selectors as cs

from .scrape import (
    get_raw_schedule_json,
    AsyncClient
)
from .database import (
    Table,
    WriteAction,
    write_db,
    writes_db
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


async def transform_from_schedule(
    conn: duckdb.Connection,
    client: AsyncClient,
    season: int
) -> int:
    '''
    Extract data from the schedules for the given season.
    Populates Venues, Teams, Games, GameStatuses.
    '''
    # TODO: consider making this function only grab one day's schedule
    #       this way, the `load` module will be able to control what days
    #       are captured based on existing data in the database
    validate_season(season)

    # use one sync request to get calendar
    season_start = DEFAULT_SEASON_START.replace(year=season)
    init_schedule = get_raw_schedule_json(season_start)
    season_json = init_schedule['page']['content']['season']
    # the calendar field for some reason doesn't get every date
    # so instead, we manually generate all dates from start to end
    rep_dates = _get_rep_dates(
        season_json['startDate'], season_json['endDate'])

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

    if len(events) == 0:
        logger.debug('No events found for %d', season)
        return 0

    # TODO: consider moving all of these to external functions
    games_inter = (
        pl.from_dicts(events)
        .lazy()
        .select(
            pl.col('id').cast(pl.Int64),
            pl.col('date')
            .str.to_datetime(time_zone='UTC').alias('datetime'),
            'tbd', 'teams', 'venue', 'status'
        )
    )

    venues = (
        games_inter
        .select(pl.col('venue').struct.unnest())
        .unnest('address')
        .select(
            pl.col('id').cast(pl.Int64),
            pl.col('fullName').alias('name'),
            'city', 'state'
        )
        .collect()
    )

    # TODO: fix detail values
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

    teams = (
        games_inter
        .select(
            pl.col('teams')
            .list.explode()
        )
        .unnest('teams')
        .unique('id')
        .select(
            pl.col('id').cast(pl.Int64),
            'location',
            pl.col('shortDisplayName').alias('mascot'),
            'abbrev',
            pl.col('teamColor').alias('color'),
            pl.col('altColor').alias('alt_color')
        )
        .collect()
    )

    games = (
        games_inter
        .with_columns(
            pl.col('teams').list.to_struct(fields=['home', 'away']),
            (
                pl.col('venue')
                .struct.field('id')
                .cast(pl.Int64)
                .alias('venue_id')
            ),
            (
                pl.col('status')
                .struct.field('id')
                .cast(pl.Int64)
                .alias('status_id')
            )
        )
        .select(
            pl.exclude('venue', 'status'),
            (
                pl.col('teams')
                .struct.field('home')
                .struct.field('id')
                .cast(pl.Int64)
                .alias('home_id')
            ),
            (
                pl.col('teams')
                .struct.field('away')
                .struct.field('id')
                .cast(pl.Int64)
                .alias('away_id')
            ),
        )
        .sort('datetime')
        .collect()
    )

    rows = writes_db(
        items=[
            (venues, Table.VENUES, WriteAction.INSERT),
            (teams, Table.TEAMS, WriteAction.INSERT),
            (game_statuses, Table.GAME_STATUSES, WriteAction.INSERT),
            (games, Table.GAMES, WriteAction.INSERT)
        ],
        conn=conn
    )

    return sum(rows)


async def transform_from_standings(
    conn: duckdb.Connection,
    client: AsyncClient,
    season: int
) -> int:
    '''
    Extract data from the standings page for the given season.
    Populates Teams, Conferences, ConferenceAlignments.
    '''
    validate_season(season)

    standings_json_raw = await client.get_raw_standings_json(season)
    standings_content = standings_json_raw['page']['content']

    conferences = (
        pl.from_dicts(
            standings_content['headerscoreboard']['collegeConfs'])
        .filter(pl.col('name').ne('NCAA Division I'))
        .select(
            pl.col('groupId').cast(pl.Int64).alias('id'),
            'name',
            pl.col('shortName').alias('abbrev')
        )
    )

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
            conferences
            .lazy()
            .select(
                pl.col('id').alias('conference_id'),
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
        .with_columns(pl.col('team_id').alias('id'))
        .select(~cs.ends_with('_id'))
        .collect()
    )

    conference_alignments = (
        teams_confs
        .select(
            'conference_id',
            pl.col('team_id'),
            pl.lit(season).alias('season')
        )
        .collect()
    )

    rows = writes_db(
        items=[
            (conferences, Table.CONFERENCES, WriteAction.INSERT),
            (teams, Table.TEAMS, WriteAction.INSERT),
            (conference_alignments, Table.CONFERENCE_ALIGNMENTS, WriteAction.INSERT)
        ],
        conn=conn
    )

    return sum(rows)


def _transform_box(json_raw: dict[str, Any],
                   team_id: int) -> pl.DataFrame:
    return (
        pl.from_dicts(json_raw['athletes'])
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


async def transform_from_game(
    conn: duckdb.Connection,
    client: AsyncClient,
    game_id: int
) -> pl.DataFrame:
    '''
    Extract data from the game page.
    Populates Plays, PlayTypes, Players, PlayerSeasons, GameLogs.
    Updates Games.
    '''
    game_json_raw = await client.get_raw_game_json(game_id)
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

    games = (
        pl.from_dicts(competition_raw)
        .select(
            pl.col('id').cast(pl.Int64),
            pl.col('neutralSite').alias('is_neutral_site'),
            pl.col('conferenceCompetition').alias('is_conference'),
            pl.col('shotChartAvailable').alias('has_shot_chart'),
            pl.lit(attendance).alias('attendance'),
            pl.col('date').str.to_date(
                format='%Y-%m-%dT%H:%MZ').alias('datetime'),
        )
    )

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
            game_id=pl.lit(game_id),
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
            timestamp=pl.col('wallclock').cast(pl.Datetime)
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

    play_types = (
        plays_inter
        .select(
            pl.col('play_type_id').alias('id'),
            pl.col('play_type_text').alias('description'),
            pl.col('is_shot')
        )
        .collect()
    )

    plays = (
        plays_inter
        .select(
            'game_id', 'sequence_id', 'play_type_id',
            'points_attempted', 'is_score',
            'team_id', 'player_id', 'assist_id',
            'period', 'clock_minutes', 'clock_seconds',
            'home_score', 'away_score', 'x_coord', 'y_coord',
            'timestamp'
        )
        .collect()
    )

    players_inter = pl.concat(
        [
            _transform_box(away_box_raw, away_id),
            _transform_box(home_box_raw, home_id)
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

    season = get_season(
        games
        .item(row=0, column='datetime')
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

    game_id = (
        games
        .item(row=0, column='id')
    )
    game_logs = (
        players_inter
        .select(
            pl.col('id').alias('player_id'),
            pl.lit(game_id).alias('game_id'),
            'played',
            'started',
            'ejected'
        )
        .collect()
    )

    rows = writes_db(
        items=[
            (games, Table.GAMES, WriteAction.UPDATE),
            (play_types, Table.PLAY_TYPES, WriteAction.INSERT),
            (players, Table.PLAYERS, WriteAction.INSERT),
            (plays, Table.PLAYS, WriteAction.INSERT),
            (player_seasons, Table.PLAYER_SEASONS, WriteAction.INSERT),
            (game_logs, Table.GAME_LOGS, WriteAction.INSERT)
        ],
        conn=conn
    )

    return sum(rows)


async def transform_from_player(
    conn: duckdb.Connection,
    client: AsyncClient,
    player_id: int
) -> int:
    '''
    Extract data from the player page.
    Updates Players.
    '''
    player_raw = await client.get_raw_player_json(player_id)
    athlete = player_raw['page']['content']['player']['plyrHdr']['ath']

    players = (
        pl.from_dict(athlete)
        .select(
            pl.lit(player_id).alias('id'),
            cs.matches(r'^htwt$')
            .str.split(', ')
            .list.to_struct(fields=['ht', 'wt'])
            .struct.unnest(),
            cs.matches(r'^dob$')
            .str.replace(r' (\d+)', ''),
            cs.matches(r'^brthpl$')
            .str.split(', ')
            .list.to_struct(fields=['birth_city', 'birth_state'])
            .struct.unnest()
        )
        .select(
            pl.exclude('htwt', 'brthpl', 'ht', 'wt'),
            cs.matches(r'^ht$')
            .str.replace_all(r'[\'"]', '')
            .str.split(' ')
            .list.to_struct(fields=['height_ft', 'height_in'])
            .struct.unnest()
            .cast(pl.Int64),
            cs.matches(r'^wt$')
            .str.replace(' lbs', '')
            .cast(pl.Int64)
            .alias('weight')
        )
    )

    rows = write_db(
        players,
        Table.PLAYERS,
        conn,
        WriteAction.UPDATE
    )

    return rows

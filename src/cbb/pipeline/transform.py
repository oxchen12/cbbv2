"""
This module provides functions for transforming raw data
from ESPN into pl.DataFrames.
"""
import asyncio
import datetime as dt
import json
import logging
from typing import Any, Iterable

import duckdb
import polars as pl
import polars.selectors as cs

from .database import (
    DBWriteTask, Table,
    WriteAction,
    get_write_tasks
)
from .date import (
    get_season,
    validate_season
)
from .extract import (
    RecordType
)
from .helpers import JSONPayload, deep_get

logger = logging.getLogger(__name__)

_SCHEDULE_KEEP = (
    'id', 'teams', 'date',
    'tbd', 'status', 'venue'
)

_VENUES_SCHEMA = {
    'id': pl.Int64,
    'name': pl.String,
    'city': pl.String,
    'state': pl.String
}

_PLAYS_INTER_SCHEMA = {
    'game_id': pl.Int64,
    'sequence_id': pl.Int64,
    'text': pl.String,
    'description': pl.String,
    'play_type_id': pl.Int64,
    'play_type_text': pl.String,
    'is_shot': pl.Boolean,
    'points_attempted': pl.Int64,
    'is_score': pl.Boolean,
    'team_id': pl.Int64,
    'player_id': pl.Int64,
    'assist_id': pl.Int64,
    'period': pl.Int64,
    'period_display': pl.String,
    'clock_minutes': pl.Int64,
    'clock_seconds': pl.Int64,
    'away_score': pl.Int64,
    'home_score': pl.Int64,
    'x_coord': pl.Int64,
    'y_coord': pl.Int64,
    'timestamp': pl.Datetime
}

_BOX_SCHEMA = {
    'player_id': pl.Int64,
    'is_totals': pl.Boolean,
    'first_name': pl.String,
    'last_name': pl.String,
    'position': pl.String,
    'started': pl.Boolean,
    'played': pl.Boolean,
    'ejected': pl.Boolean,
    'jersey': pl.Int64,
    'team_id': pl.Int64,
    'stats': pl.Struct
}

_GAMES_SCHEMA = {
    'id': pl.Int64,
    'is_neutral_site': pl.Boolean,
    'is_conference': pl.Boolean,
    'has_shot_char': pl.Boolean,
    'attendance': pl.Int64,
    'datetime': pl.Datetime,
    'complete_record': pl.Boolean
}


def _col_if_exists(name: str, *more_names: str) -> cs.Selector:
    """
    Selector for an optional column.
    Returns an empty selector if the column does not exist.
    """
    return cs.matches(f'^{"|".join([name, *more_names])}$')


def _with_default_col(
    df: pl.DataFrame | pl.LazyFrame,
    name: str,
    default: pl.Expr
) -> pl.DataFrame | pl.LazyFrame:
    if isinstance(df, pl.LazyFrame):
        columns = df.collect_schema().names()
    else:
        columns = df.columns

    if name in columns:
        return df
    return df.with_columns(default.alias(name))


def _empty_df_with_schema(schema: dict[str, type[pl.DataType]]) -> pl.DataFrame:
    return pl.DataFrame([], schema=schema)


async def _fetch_payload(
    store_conn: duckdb.DuckDBPyConnection,
    document_key: Any,
    record_type: RecordType
) -> JSONPayload:
    document_key = record_type.raw_key_to_str(document_key)
    res = store_conn.sql(
        'SELECT payload\n'
        'FROM Documents\n'
        'WHERE document_key = $document_key AND record_type = $record_type\n'
        'ORDER BY timestamp DESC\n'
        'LIMIT 1',
        params={'document_key': document_key, 'record_type': record_type.label}
    )
    payload = res.fetchone()[0]
    if payload is None:
        return {}
    return json.loads(payload)


async def transform_from_schedule(
    store_conn: duckdb.DuckDBPyConnection,
    write_queue: asyncio.Queue[Iterable[DBWriteTask]],
    rep_date: dt.date
):
    """
    Extract data from the schedules for the given (representative) date.
    Populates Venues, Teams, Games, GameStatuses.
    """
    schedule_json_raw = await _fetch_payload(
        store_conn,
        rep_date,
        RecordType.SCHEDULE
    )

    date_schedules = deep_get(
        schedule_json_raw,
        'page', 'content', 'events',
        default={}
    ).values()
    events = [
        {
            k: v
            for k, v in event.items()
            if k in _SCHEDULE_KEEP
        }
        for date_schedule in date_schedules
        for event in date_schedule
    ]

    if len(events) == 0:
        logger.debug('No events found for %s', rep_date)
        return 0

    # TODO: consider moving all of these to external functions
    games_inter = (
        pl.from_dicts(events)
        .lazy()
        .select(
            pl.col('id').str.to_integer(strict=False),
            pl.col('date')
            .str.to_datetime(time_zone='UTC').alias('datetime'),
            'tbd',
            'teams',
            _col_if_exists('venue'),
            'status'
        )
    )

    venues = _empty_df_with_schema(_VENUES_SCHEMA)
    if 'venue' in games_inter.collect_schema().names():
        venues = (
            games_inter
            .select(pl.col('venue').struct.unnest())
            .select(
                pl.col('id').str.to_integer(strict=False),
                pl.col('fullName').alias('name'),
                _col_if_exists('address').struct.field('city'),
                _col_if_exists('address').struct.field('state')
            )
            .collect()
        )

    # TODO: fix detail values
    game_statuses = (
        games_inter
        .select(pl.col('status').struct.unnest())
        .select(
            pl.col('id').str.to_integer(strict=False),
            'state',
            # remove details with OT specified
            pl.col('detail').str.replace(r'(.+)/\d*OT$', r'$1')
        )
        .unique()
        .sort('id')
        .collect()
    )

    # TODO: find where to get the color from
    teams = (
        games_inter
        .select(
            pl.col('teams')
            .list.explode()
        )
        .unnest('teams')
        .unique('id')
        .select(
            pl.col('id').str.to_integer(strict=False),
            'location',
            pl.col('shortDisplayName').alias('mascot'),
            'abbrev',
            _col_if_exists('teamColor').alias('color'),
            _col_if_exists('altColor').alias('alt_color')
        )
        .collect()
    )

    games = (
        games_inter
        .with_columns(
            pl.col('teams').list.to_struct(fields=['home', 'away']),
            (
                _col_if_exists('venue')
                .struct.field('id')
                .str.to_integer(strict=False)
                .alias('venue_id')
            ),
            (
                pl.col('status')
                .struct.field('id')
                .str.to_integer(strict=False)
                .alias('status_id')
            ),
            # TODO: find better solution for jank hotfix
            #       |- I have to use None here to ensure it passes
            #       |- the update check in database.py
            pl.lit(None).alias('complete_record')
        )
        .select(
            pl.exclude('venue', 'status'),
            (
                pl.col('teams')
                .struct.field('home')
                .struct.field('id')
                .str.to_integer(strict=False)
                .alias('home_id')
            ),
            (
                pl.col('teams')
                .struct.field('away')
                .struct.field('id')
                .str.to_integer(strict=False)
                .alias('away_id')
            ),
        )
        .sort('datetime')
        .collect()
    )

    tasks = get_write_tasks(
        (venues, Table.VENUES, WriteAction.INSERT),
        (teams, Table.TEAMS, WriteAction.INSERT),
        (game_statuses, Table.GAME_STATUSES, WriteAction.INSERT),
        (games, Table.GAMES, WriteAction.INSERT)
    )

    await write_queue.put(tasks)

    return None


async def transform_from_standings(
    store_conn: duckdb.DuckDBPyConnection,
    write_queue: asyncio.Queue[Iterable[DBWriteTask]],
    season: int
):
    """
    Extract data from the standings page for the given season.
    Populates Teams, Conferences, ConferenceAlignments.
    """
    if not validate_season(season):
        logging.warning('Got invalid season: %d', season)
        return -1

    standings_json_raw = await _fetch_payload(
        store_conn,
        season,
        RecordType.STANDINGS
    )
    standings_content = deep_get(standings_json_raw, 'page', 'content')

    if standings_content is None:
        logging.warning('Got invalid standings page: %d', season)
        return -1

    conferences = (
        pl.from_dicts(
            deep_get(
                standings_content, 'headerscoreboard',
                'collegeConfs', default={}
            )
        )
        .filter(pl.col('name').ne('NCAA Division I'))
        .select(
            pl.col('groupId').str.to_integer(strict=False).alias('id'),
            'name',
            pl.col('shortName').alias('abbrev')
        )
    )

    def _extract_team(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Extracts the team columns from the standings."""
        return (
            lf
            .filter(pl.col('standings').is_not_null())
            .explode('standings')
            .unnest('standings')
            .unnest('team')
        )

    teams_confs_base = (
        pl.from_dicts(
            deep_get(
                standings_content, 'standings',
                'groups', 'groups', default={}
            )
        )
        .lazy()
        .select(
            pl.col('name').alias('confName'),
            pl.col('standings'),
            _col_if_exists('children')
        )
    )

    teams_confs_no_div = (
        teams_confs_base
        .select('confName', 'standings')
        .pipe(_extract_team)
    )

    # hack to get initialize with an empty LazyFrame
    teams_confs_div = teams_confs_no_div.limit(0)
    if 'children' in teams_confs_base.collect_schema().names():
        teams_confs_div = (
            teams_confs_base
            .explode('children')
            .select(
                'confName',
                pl.col('children')
                .struct.field('standings')
            )
            .pipe(_extract_team)
        )

    teams_confs = (
        pl.concat(
            [teams_confs_no_div, teams_confs_div],
            how='diagonal_relaxed'
        )
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
            pl.col('id').alias('team_id').str.to_integer(strict=False),
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

    tasks = get_write_tasks(
        (conferences, Table.CONFERENCES, WriteAction.INSERT),
        (teams, Table.TEAMS, WriteAction.INSERT),
        (conference_alignments, Table.CONFERENCE_ALIGNMENTS, WriteAction.INSERT)
    )

    await write_queue.put(tasks)

    return None


def _transform_box(
    box_json_raw: dict[str, Any],
    team_id: int
) -> pl.LazyFrame:
    box = _empty_df_with_schema(_BOX_SCHEMA).lazy()
    athletes = deep_get(box_json_raw, 'athletes')
    if (
        athletes is None
        or len(athletes) == 0
    ):
        return box

    first_athlete = deep_get(athletes[0], 'athlete')
    if (
        first_athlete is None
        or 'id' not in first_athlete
    ):
        return box

    stat_names = box_json_raw.get('names')

    def cast_stats(df: pl.LazyFrame) -> pl.LazyFrame:
        def cast_box_stat(expr: pl.Expr) -> pl.Expr:
            return (
                expr
                .cast(pl.String)
                .str.to_integer(strict=False)
            )

        return (
            df
            .with_columns(
                pl.col('stats').list.to_struct(fields=stat_names)
            )
            .unnest('stats', separator='_')
            .select(
                ~cs.starts_with('stats'),
                # pull out stats and perform ops there due to issues with nesting
                pl.struct(
                    pl.col('stats_MIN').pipe(cast_box_stat).alias('minutes'),
                    pl.col('stats_PTS').pipe(cast_box_stat).alias('points'),
                    pl.col('stats_OREB').pipe(
                        cast_box_stat
                    ).alias('off_rebounds'),
                    pl.col('stats_DREB').pipe(
                        cast_box_stat
                    ).alias('def_rebounds'),
                    pl.col('stats_AST').pipe(cast_box_stat).alias('assists'),
                    pl.col('stats_TO').pipe(cast_box_stat).alias('turnovers'),
                    pl.col('stats_STL').pipe(cast_box_stat).alias('steals'),
                    pl.col('stats_PF').pipe(
                        cast_box_stat
                    ).alias('personal_fouls'),
                    pl.col('stats_FG')
                    .cast(pl.String)
                    .str.split('-')
                    .list.eval(pl.element().str.to_integer(strict=False))
                    .list.to_struct(fields=['fg_made', 'fg_attempted'])
                    .struct.unnest(),
                    pl.col('stats_3PT')
                    .cast(pl.String)
                    .str.split('-')
                    .list.eval(pl.element().str.to_integer(strict=False))
                    .list.to_struct(fields=['fg3_made', 'fg3_attempted'])
                    .struct.unnest()
                )
                .alias('stats')
            )
        )

    players_box = (
        pl.from_dicts(athletes)
        .lazy()
        .unnest('athlete')
        .with_columns(
            pl.col('shortName')
            .str.extract(r'[A-Za-z]+\. (.+)')
            .alias('last_name'),
        )
        .select(
            pl.col('id').alias('player_id').str.to_integer(strict=False),
            pl.lit(False).alias('is_totals'),
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
            _col_if_exists('jersey').str.to_integer(strict=False),
            pl.lit(team_id).alias('team_id'),
            pl.col('stats')  # .pipe(cast_stats)
        )
        .pipe(cast_stats)
    )

    stat_totals = box_json_raw.get('totals')

    team_box = (
        pl.LazyFrame(
            {
                'is_totals': [True],
                'team_id': [team_id],
                'stats': [stat_totals]
            },
        )
        # .with_columns(
        #     pl.col('stats')# .pipe(cast_stats)
        # )
        .pipe(cast_stats)
    )

    box = (
        pl.concat(
            [
                players_box,
                team_box
            ],
            how='diagonal_relaxed'
        )
    )

    if 'jersey' not in box.collect_schema().names():
        box = (
            box
            .with_columns(
                pl.lit(None, dtype=pl.Int64).alias('jersey')
            )
            .select(list(_BOX_SCHEMA.keys()))
        )
    return box


def _get_box_inter(game_json_raw: dict[str, Any]) -> pl.LazyFrame:
    players_content = deep_get(
        game_json_raw,
        'boxscore', 'players'
    )
    if players_content is None:
        return _empty_df_with_schema(_BOX_SCHEMA).lazy()

    away_id, home_id = (
        int(deep_get(x, 'team', 'id', default=-1))
        for x in players_content
    )
    away_box_raw, home_box_raw = (
        # TODO: error handling?
        x.get('statistics')[0]
        for x in players_content
    )

    try:
        players_inter = pl.concat(
            [
                _transform_box(away_box_raw, away_id),
                _transform_box(home_box_raw, home_id)
            ],
            how='vertical_relaxed'
        )
        players_inter.head(10).collect()
    except pl.exceptions.PolarsError as e:
        raise e

    return players_inter


async def transform_from_game(
    store_conn: duckdb.DuckDBPyConnection,
    write_queue: asyncio.Queue[Iterable[DBWriteTask]],
    game_id: int
):
    """
    Extract data from the game page.
    Populates Plays, PlayTypes, Players, PlayerSeasons, GameLogs.
    Updates Games.
    """
    game_json_raw = await _fetch_payload(
        store_conn,
        game_id,
        RecordType.GAME
    )
    if game_json_raw is None:
        logger.warning('Couldn\'t get results for game id %d', game_id)
        return -1
    attendance = deep_get(game_json_raw, 'gameInfo', 'attendance')
    competition_raw = deep_get(game_json_raw, 'header', 'competitions')

    games = _empty_df_with_schema(_GAMES_SCHEMA)
    if competition_raw is not None:
        games = (
            pl.from_dicts(competition_raw)
            .select(
                pl.col('id').str.to_integer(strict=False),
                _col_if_exists('neutralSite').alias('is_neutral_site'),
                _col_if_exists('conferenceCompetition').alias('is_conference'),
                _col_if_exists('shotChartAvailable').alias('has_shot_chart'),
                pl.lit(attendance).alias('attendance'),
                pl.col('date')
                .cast(pl.String)
                .str.to_date(
                    format='%Y-%m-%dT%H:%MZ'
                ).alias('datetime'),
                pl.lit(attendance is not None).alias('complete_record')
            )
        )

    plays_inter = _empty_df_with_schema(_PLAYS_INTER_SCHEMA).lazy()
    plays_raw = game_json_raw.get('plays', None)
    if (
        plays_raw is not None
        and len(plays_raw) > 0
    ):
        plays_inter = (
            pl.from_dicts(plays_raw)
            .lazy()
            .unnest(
                'type',
                'period',
                'clock',
                _col_if_exists('coordinate'),
                _col_if_exists('team'),
                separator='_'
            )
            .with_columns(
                pl.col('clock_displayValue').str.split(':')
            )
            .with_columns(
                _col_if_exists('coordinate_x').alias('x_coord'),
                _col_if_exists('coordinate_y').alias('y_coord'),
                _col_if_exists('wallclock').cast(pl.Datetime),
                _col_if_exists('team_id').str.to_integer(strict=False),
                game_id=pl.lit(game_id),
                player_id=(
                    _col_if_exists('participants')
                    .list.first()
                    .struct.unnest()
                    .struct.unnest()
                    .str.to_integer(strict=False)
                ),
                assist_id=(
                    pl.when(_col_if_exists('participants').list.len().gt(1))
                    .then(
                        _col_if_exists('participants')
                        .list.last()
                        .struct.unnest()
                        .struct.unnest()
                        .str.to_integer(strict=False)
                    )
                    .otherwise(None)
                ),
                period=pl.col('period_number'),
                period_display=pl.col('period_displayValue'),
                sequence_id=pl.col(
                    'sequenceNumber'
                ).str.to_integer(strict=False),
                play_type_id=pl.col('type_id').str.to_integer(strict=False),
                play_type_text=pl.col('type_text'),
                clock_minutes=pl.col(
                    'clock_displayValue'
                ).list.get(0).str.to_integer(strict=False),
                clock_seconds=pl.col(
                    'clock_displayValue'
                ).list.get(1).str.to_integer(strict=False),
                description=pl.col('shortDescription'),
                away_score=pl.col('awayScore'),
                home_score=pl.col('homeScore'),
                is_shot=pl.col('shootingPlay'),
                is_score=pl.col('scoringPlay'),
                points_attempted=pl.col('pointsAttempted')
            )
            .select(
                'game_id', 'sequence_id', 'text',
                'description', 'play_type_id', 'play_type_text',
                'is_shot', 'points_attempted', 'is_score',
                'period', 'period_display',
                'clock_minutes', 'clock_seconds',
                'away_score', 'home_score',
                _col_if_exists(
                    'x_coord',
                    'y_coord',
                    'timestamp',
                    'team_id',
                    'player_id',
                    'assist_id'
                )
            )
        )

    play_types = (
        plays_inter
        .select(
            pl.col('play_type_id').alias('id'),
            pl.col('play_type_text').alias('description'),
            pl.col('is_shot')
        )
        .unique('id')
        .collect()
    )

    plays = (
        plays_inter
        .select(
            'game_id', 'sequence_id', 'play_type_id',
            'points_attempted', 'is_score',
            'period', 'clock_minutes', 'clock_seconds',
            'home_score', 'away_score',
            _col_if_exists(
                'x_coord',
                'y_coord',
                'timestamp',
                'team_id',
                'player_id',
                'assist_id'
            )
        )
        .collect()
    )

    box_inter = _get_box_inter(game_json_raw)
    players_inter = box_inter.filter(~pl.col('is_totals'))
    teams_inter = box_inter.filter(pl.col('is_totals'))

    players = (
        players_inter
        .select(
            pl.col('player_id').alias('id'),
            'first_name',
            'last_name',
            'position',
            # TODO: find better solution for jank hotfix
            #       |- I have to use None here to ensure it passes
            #       |- the update check in database.py
            pl.lit(None).alias('complete_record')
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
            'player_id',
            'team_id',
            pl.lit(season).alias('season'),
            _col_if_exists('jersey')
        )
        .collect()
    )

    game_logs = (
        players_inter
        .select(
            'player_id',
            pl.lit(game_id).alias('game_id'),
            'played',
            'started',
            'ejected'
        )
        .collect()
    )

    player_box_scores = (
        players_inter
        .select(
            'player_id',
            pl.lit(game_id).alias('game_id'),

        )
        .collect()
    )

    team_box_scores = (
        teams_inter
        .select(
            'team_id',
            pl.lit(game_id).alias('game_id'),
            pl.col('stats')
            .struct.unnest()
        )
        .collect()
    )

    tasks = get_write_tasks(
        (games, Table.GAMES, WriteAction.UPDATE),
        (play_types, Table.PLAY_TYPES, WriteAction.INSERT),
        (players, Table.PLAYERS, WriteAction.INSERT),
        (plays, Table.PLAYS, WriteAction.INSERT),
        (player_seasons, Table.PLAYER_SEASONS, WriteAction.INSERT),
        (game_logs, Table.GAME_LOGS, WriteAction.INSERT),
        (player_box_scores, Table.PLAYER_BOX_SCORES, WriteAction.INSERT),
        (team_box_scores, Table.TEAM_BOX_SCORES, WriteAction.INSERT),
    )

    await write_queue.put(tasks)

    return None


async def transform_from_player(
    store_conn: duckdb.DuckDBPyConnection,
    write_queue: asyncio.Queue[Iterable[DBWriteTask]],
    player_id: int
):
    """
    Extract data from the player page.
    Updates Players.
    """
    player_raw = await _fetch_payload(
        store_conn,
        player_id,
        RecordType.PLAYER
    )
    athlete = deep_get(
        player_raw,
        'page',
        'content',
        'player',
        'plyrHdr',
        'ath'
    )

    if athlete is None:
        logger.debug('Player with id %d was not found', player_id)
        return 0

    players = (
        pl.from_dict(athlete)
        .select(
            pl.lit(player_id).alias('id'),
            _col_if_exists('htwt')
            .str.split(', ')
            .list.to_struct(fields=['ht', 'wt'])
            .struct.unnest(),
            # _col_if_exists('dob')
            # .str.replace(r' (\d+)', ''),
            _col_if_exists('brthpl')
            .str.split(', ')
            .list.to_struct(fields=['birth_city', 'birth_state'])
            .struct.unnest()
        )
        .select(
            pl.exclude('htwt', 'brthpl', 'ht', 'wt'),
            _col_if_exists('ht')
            .str.replace_all(r'[\'"]', '')
            .str.split(' ')
            .list.to_struct(fields=['height_ft', 'height_in'])
            .struct.unnest()
            .str.to_integer(strict=False),
            _col_if_exists('wt')
            .str.replace(' lbs', '')
            .str.to_integer(strict=False)
            .alias('weight'),
            pl.lit(True).alias('complete_record')
        )
    )

    tasks = get_write_tasks(
        (players, Table.PLAYERS, WriteAction.UPDATE),
    )

    await write_queue.put(tasks)

    return None
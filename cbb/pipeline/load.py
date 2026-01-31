"""
This module provides functions for loading transformed
data from ESPN into a local SQLite database.
"""
from itertools import batched
from typing import Collection, Any, Sequence
import asyncio
import datetime as dt
import logging
import math

from tqdm import tqdm
import aiohttp
import duckdb
import polars as pl

from .database import (
    get_affected_rows, write_db, Table, WriteAction
)
from .date import (
    fix_season_range_ends,
    get_season_start,
    MIN_SEASON,
    MAX_SEASON,
    CALENDAR_DT_FORMAT
)
from .extract import (
    AsyncClient,
    is_non_transient
)

from .transform import (
    transform_from_schedule,
    transform_from_standings,
    transform_from_game,
    transform_from_player
)
from ._async import (
    MAX_TRANSFORM_COROUTINES
)

logger = logging.getLogger(__name__)


async def _batch_load(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    load_func,
    args: Collection[Any],
    max_transform_coroutines: int = MAX_TRANSFORM_COROUTINES,
    disable_tqdm: bool = False,
    return_exceptions: bool = False
) -> list[int]:
    """
    Process the loading function in batches.
    """
    results = []
    for batch in tqdm(
        batched(args, max_transform_coroutines),
        total=math.ceil(len(args) / max_transform_coroutines),
        disable=disable_tqdm
    ):
        tasks = [
            load_func(conn, client, arg)
            for arg in batch
        ]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for arg, res in zip(args, batch_results):
            if not isinstance(res, Exception):
                continue
            if isinstance(res, asyncio.TimeoutError):
                logger.warning('Timed out for arg %s', arg)
            elif isinstance(res, aiohttp.ClientResponseError):
                logger.debug('Got %d from request for arg %s', res.status, arg)
            else:
                logger.warning('An error occurred for arg %s: %s', arg, res)

        if not return_exceptions:
            batch_results = [
                -1 if isinstance(res, Exception) else res
                for res in batch_results
            ]

        results.extend(batch_results)
    return results


def _get_rep_dates(
    start: str,
    end: str
) -> list[dt.date]:
    """
    Get the necessary dates to fetch between start and end from schedules.
    Assumes start and end are formatted like CALENDAR_DT_FORMAT.
    """
    start_date = dt.datetime.strptime(start, CALENDAR_DT_FORMAT).date()
    start_date = max(
        start_date,
        get_season_start(start_date.year)
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


async def _get_rep_dates_json(init_schedule: dict[str, Any]) -> list[dt.date]:
    """Get the representative dates from the raw initial schedule."""
    season_json = init_schedule['page']['content']['season']
    # the calendar field for some reason doesn't get every date
    # so instead, we manually generate all dates from start to end
    rep_dates = _get_rep_dates(
        season_json['startDate'], season_json['endDate']
    )

    return rep_dates


async def load_schedule_range(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON
) -> int:
    """
    Load schedule data from the seasons in a given range.
    """
    start_season, end_season = fix_season_range_ends(start_season, end_season)
    logger.debug('Loading schedules for seasons %d to %d',
                 start_season, end_season)
    seasons = range(start_season, end_season + 1)
    season_starts = [
        get_season_start(season)
        for season in seasons
    ]
    init_schedule_tasks = [
        client.get_raw_schedule_json(season_start)
        for season_start in season_starts
    ]
    init_schedules = await asyncio.gather(*init_schedule_tasks)
    season_rep_date_tasks = [
        _get_rep_dates_json(init_schedule)
        for init_schedule in init_schedules
    ]
    season_rep_dates = await asyncio.gather(*season_rep_date_tasks)
    rep_dates = [date for srd in season_rep_dates for date in srd]
    rows = await _batch_load(
        conn, client, transform_from_schedule, rep_dates,
    )

    return get_affected_rows(rows)


async def load_standings_range(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON,
    disable_tqdm: bool = False
) -> int:
    """
    Load standings data for seasons between `start_season` and `end_season`.
    """
    start_season, end_season = fix_season_range_ends(start_season, end_season)
    logger.debug('Loading standings for seasons %d to %d',
                 start_season, end_season)
    tasks = [
        transform_from_standings(conn, client, season)
        for season in tqdm(
            range(start_season, end_season + 1),
            total=end_season - start_season + 1,
            disable=disable_tqdm
        )
    ]
    rows = await asyncio.gather(*tasks)

    return get_affected_rows(rows)


def _get_season_start_str(season: int) -> str:
    return get_season_start(season).strftime('\'%Y-%m-%d\'')


def _extract_column_from_query(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    col: str,
    dtype: pl.DataType = pl.Int32
) -> pl.Series:
    return (
        conn.sql(query)
        .pl()
        .get_column(col)
        .cast(dtype)
    )


def _mark_non_transient_complete(
    conn: duckdb.DuckDBPyConnection,
    results: list[int | Exception],
    ids: Sequence[int],
    table: Table,
    id_col: str = 'id'
) -> list[int]:
    """
    Marks results with non-transient issues as complete.
    Modifies `results` list to convert exceptions to plain -1.
    """
    non_transient_ids = []
    for i, res in enumerate(results):
        if not isinstance(res, Exception):
            continue
        results[i] = -1
        if isinstance(res, aiohttp.ClientResponseError):
            if is_non_transient(res.status):
                non_transient_ids.append(ids[i])

    non_transient_df = (
        pl.Series(non_transient_ids)
        .alias(id_col)
        .to_frame()
        .with_columns(
            pl.lit(False).alias('complete_record')
        )
    )

    rows = write_db(
        non_transient_df,
        table,
        conn,
        WriteAction.UPDATE
    )
    logger.debug('Marked %d rows complete', rows)

    return results

async def update_games(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    game_ids: Sequence[int]
) -> list[int]:
    """
    Updates existing game rows.
    """
    game_res = await _batch_load(
        conn, client, transform_from_game, game_ids,
        return_exceptions=True
    )
    game_res = _mark_non_transient_complete(
        conn,
        game_res,
        game_ids,
        Table.GAMES
    )

    return game_res

async def update_games_seasons(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON
) -> list[int]:
    """
    Updates existing game rows in the given seasons.
    """
    query_select_game_ids = (
        'SELECT id\n'
        'FROM Games\n'
        f'WHERE datetime >= {_get_season_start_str(start_season)}\n'
        f'AND datetime < {_get_season_start_str(end_season + 1)}\n'
        'AND complete_record IS NULL;'
    )
    game_ids = _extract_column_from_query(conn, query_select_game_ids, 'id')

    return await update_games(conn, client, game_ids)

async def update_players(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    player_ids: Sequence[int]
) -> list[int]:
    """
    Updates existing player rows.
    """
    player_res = await _batch_load(
        conn, client, transform_from_player, player_ids,
        return_exceptions=True
    )
    player_res = _mark_non_transient_complete(
        conn,
        player_res,
        player_ids,
        Table.PLAYERS
    )

    return player_res

async def update_players_seasons(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON
) -> list[int]:
    """
    Updates existing player rows in the given seasons.
    """
    query_select_player_ids = (
        'SELECT DISTINCT player_id\n'
        'FROM PlayerSeasons PS\n'
        'JOIN Players P ON PS.player_id = P.id\n'
        f'WHERE season >= {start_season}\n'
        f'AND season <= {end_season}\n'
        'AND complete_record IS NULL;'
    )
    player_ids = _extract_column_from_query(
        conn, query_select_player_ids, 'player_id'
    )

    return await update_players(conn, client, player_ids)


async def load_all(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON,
    _seasons_complete: bool = False
) -> int:
    """
    Load all the data from `start_season` to `end_season`
    into the database.
    """
    results = []

    # TODO: add additional db ops in these functions that mark a meta-table
    #       to show that the season has been successfully filled
    if not _seasons_complete:
        # TODO: load standings
        standings_res = await load_standings_range(conn, client, start_season, end_season)
        # TODO: load schedules
        schedule_res = await load_schedule_range(conn, client, start_season, end_season)
        results.append(standings_res)
        results.append(schedule_res)

    # TODO: update games
    game_res = await update_games_seasons(conn, client, start_season, end_season)
    results.append(game_res)

    # TODO: update players
    player_res = await update_players_seasons(conn, client, start_season, end_season)
    results.append(player_res)

    # TODO: not yet implemented
    rows = [get_affected_rows(res) for res in results]
    return get_affected_rows(rows)

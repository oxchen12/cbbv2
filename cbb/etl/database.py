'''
This module provides functions and classes for database
operations within the parent module.
'''
from contextlib import closing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Sequence
import logging

from adbc_driver_manager import dbapi
import adbc_driver_sqlite
import polars as pl

logger = logging.getLogger(__name__)

SUBMODULE_DIR = Path(__file__).parent
SQL_DIR = SUBMODULE_DIR / 'sql'

DB_DIR = Path.cwd() / 'db'
DB_FILENAME = 'cbb.db'
DB_FILE = DB_DIR / DB_FILENAME


@dataclass(frozen=True)
class _TableSpec:
    '''Represents the specification for a table.'''
    name: str
    primary_key: Sequence[str]


class Table(Enum):
    '''
    Abstraction for a SQL table.
    Includes table name and primary key.
    '''
    GAMES = _TableSpec('Games', ['id'])
    GAME_STATUSES = _TableSpec('GameStatuses', ['id'])
    VENUES = _TableSpec('Venues', ['id'])
    TEAMS = _TableSpec('Teams', ['id'])
    CONFERENCE_ALIGNMENTS = _TableSpec(
        'ConferenceAlignments', ['team_id', 'conference_id', 'season'])
    CONFERENCES = _TableSpec('Conferences', ['id'])
    PLAYS = _TableSpec('Plays', ['game_id', 'sequence_id'])
    PLAY_TYPES = _TableSpec('PlayTypes', ['id'])
    PLAYERS = _TableSpec('Players', ['id'])
    PLAYER_SEASONS = _TableSpec(
        'PlayerSeasons', ['player_id', 'team_id', 'season'])
    GAME_LOGS = _TableSpec('GameLogs', ['player_id', 'game_id'])


def _get_insert_query(
    df: pl.DataFrame,
    table_spec: _TableSpec,
) -> str:
    '''Returns the template insert query based on the DataFrame.'''
    cols_spec = ', '.join(df.columns)
    dummy_spec = ', '.join(['?'] * len(df.columns))
    query = (
        f'INSERT INTO {table_spec.name} ({cols_spec}) '
        f'VALUES ({dummy_spec})'
    )

    return query


def _execute_insert_query(
    df: pl.DataFrame,
    conn: dbapi.Connection,
    query: str
) -> int:
    '''Executes the query on the connection.'''
    logger.debug('Executing %s', query)
    with (
        conn,
        closing(conn.cursor()) as cursor
    ):
        cursor.executemany(query, df.iter_rows())

    return rows


def insert_to_db(
    df: pl.DataFrame,
    table: Table,
    conn: dbapi.Connection,
    on_conflict: str = 'nothing'
) -> int:
    '''
    Inserts the rows from the DataFrame into the specified table.
    Uses the names of the columns in the DataFrame to construct the query.

    Specify `on_conflict` to control conflict behavior.
    '''
    table_spec = table.value
    insert_query = _get_insert_query(df, table_spec)
    pk_str = ', '.join(table_spec.primary_key)

    if on_conflict == 'update':
        set_spec = ',\n'.join(
            f'\t{col}=excluded.{col}'
            for col in df.columns
        )
        query = (
            f'{insert_query}\n'
            f'ON CONFLICT ({pk_str}) DO UPDATE SET\n'
            f'{set_spec};'
        )
    else:
        query = (
            f'{insert_query}\n'
            f'ON CONFLICT ({pk_str}) DO NOTHING;'
        )

    rows = _execute_insert_query(df, conn, query)

    if rows == -1:
        logger.debug('Failed to insert rows to %s', table_spec.name)
    else:
        logger.debug('Inserted %s rows into %s', rows, table_spec.name)

    return rows


def inserts_to_db(
    items: list[tuple[pl.DataFrame, Table, str]],
    conn: dbapi.Connection,
) -> list[int]:
    '''Inserts multiple DataFrames into the specified tables.'''
    # TODO: make this concurrent
    rows = []
    for df, table, on_conflict in items:
        rows.append(
            insert_to_db(
                df, table, conn,
                on_conflict=on_conflict
            )
        )

    return rows


def init_db(
    conn: dbapi.Connection,
    erase: bool = False
):
    '''
    (Re-)initializes the database file.
    If `erase` is True and file exists, erases the old DB.
    '''
    if erase and DB_FILE.exists():
        resp = ''
        while resp not in ('y', 'n'):
            usr_in = input(
                f'Are you sure you want to delete {DB_FILENAME}? (Y/[N]) '
            ).strip()

            resp = usr_in.lower()
            if usr_in == '':
                resp = 'n'
            resp = resp[0]

        if resp == 'y':
            logger.debug('Deleting old %s', DB_FILENAME)
            DB_FILE.unlink(missing_ok=True)
        else:
            logger.debug('Keeping old %s', DB_FILENAME)

    try:
        with (
            conn,
            open(
                SQL_DIR / 'create_tables.sql',
                mode='r+',
                encoding='utf-8'
            ) as sql_fp
        ):
            cursor = conn.cursor()
            sql_script = sql_fp.read()
            cursor.executescript(sql_script)

        logging.debug('Successfully initialized %s', DB_FILENAME)
    except Exception as e:
        logging.debug('An error occurred: %s', e)


if __name__ == '__main__':
    main()

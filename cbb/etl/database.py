'''
This module provides functions and classes for database
operations within the parent module.
'''
from contextlib import closing
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Sequence
import logging

import duckdb
import polars as pl

logger = logging.getLogger(__name__)

SUBMODULE_DIR = Path(__file__).parent
SQL_DIR = SUBMODULE_DIR / 'sql'
INIT_FILENAME = 'create_tables.sql'
INIT_FILE = SQL_DIR / INIT_FILENAME

DB_DIR = Path.cwd() / 'db'
DB_FILENAME = 'cbb.duckdb'
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


class WriteAction(Enum):
    INSERT = auto()
    UPDATE = auto()
    UPSERT = auto()


def _get_insert_query(
    df: pl.DataFrame,
    table_spec: _TableSpec,
) -> str:
    '''Returns the template insert query based on the DataFrame.'''
    cols_spec = ', '.join(df.columns)
    dummy_spec = ', '.join(f'{col}' for col in df.columns)
    query = (
        f'INSERT INTO {table_spec.name} ({cols_spec}) '
        f'SELECT {cols_spec} '
        f'FROM df'
    )

    return query


def _get_update_query(
    df: pl.DataFrame,
    table_spec: _TableSpec
) -> str:
    '''Returns the update query based on the DataFrame.'''
    update_set_spec = ', '.join(
        f'{col} = df.{col}'
        for col in df.columns
        if col not in table_spec.primary_key
    )
    update_pk_spec = ' AND '.join(
        f'{table_spec.name}.{col} = df.{col}'
        for col in df.columns
        if col in table_spec.primary_key
    )

    query = (
        f'UPDATE {table_spec.name}\n'
        f'SET {update_set_spec}\n'
        f'FROM df\n'
        f'WHERE {update_pk_spec}\n;'
    )

    return query


def _execute_write_query(
    df: pl.DataFrame,
    conn: duckdb.Connection,
    query: str
) -> int:
    '''Executes the query on the connection.'''
    try:
        res = conn.execute(query)
        rows = res.fetchone()[0]
    except duckdb.Error as e:
        logger.debug('An error occurred during query execution: %s', e)
        logger.debug(query)
        rows = -1

    return rows


def write_db(
    df: pl.DataFrame,
    table: Table,
    conn: duckdb.Connection,
    write_action: WriteAction = WriteAction.INSERT
) -> int:
    '''
    Inserts the rows from the DataFrame into the specified table.
    Uses the names of the columns in the DataFrame to construct the query.

    Specify `on_conflict` to control conflict behavior.
    '''
    table_spec = table.value
    insert_query = _get_insert_query(df, table_spec)
    pk_str = ', '.join(table_spec.primary_key)

    match write_action:
        case WriteAction.UPSERT:
            upsert_set_spec = ',\n'.join(
                f'\t{col}=EXCLUDED.{col}'
                for col in df.columns
                if col not in table_spec.primary_key
            )
            query = (
                f'{insert_query}\n'
                f'ON CONFLICT ({pk_str}) DO UPDATE SET\n'
                f'{upsert_set_spec};'
            )
        case WriteAction.UPDATE:
            query = _get_update_query(df, table_spec)
        case _:
            query = (
                f'{insert_query}\n'
                f'ON CONFLICT ({pk_str}) DO NOTHING;'
            )

    rows = _execute_write_query(df, conn, query)

    if rows == -1:
        logger.debug('Failed to insert rows to %s', table_spec.name)
    else:
        match write_action:
            case WriteAction.UPSERT:
                write_action_str = 'Upserted'
            case WriteAction.UPDATE:
                write_action_str = 'Updated'
            case _:
                write_action_str = 'Inserted'

        logger.debug(
            '%s %d rows (of %d) in %s',
            write_action_str,
            rows,
            df.height,
            table_spec.name
        )

    return rows


def writes_db(
    items: list[tuple[pl.DataFrame, Table, WriteAction]],
    conn: duckdb.Connection,
) -> list[int]:
    '''
    Inserts multiple DataFrames into the specified tables.
    Returns the number of affected rows.
    '''
    # TODO: find a database package to do concurrent writes
    rows = []
    for df, table, on_conflict in items:
        rows.append(
            write_db(
                df, table, conn,
                write_action=on_conflict
            )
        )

    results = zip(items, rows)
    failed_tables = [res[0][1].value.name for res in results if res[1] == -1]
    if len(failed_tables) > 0:
        logger.debug(
            'Failed to insert to the following tables: %s', failed_tables)

    success_rows = sum(x for x in rows if x >= 0)
    logger.debug('Affected %s rows', success_rows)

    return rows


def _delete_db(db_file: Path) -> bool:
    '''Deletes the existing database file.'''
    try:
        db_file.unlink()
        return True
    except PermissionError as e:
        logger.debug('Another process is using %s, aborting', db_file.name)
        return False


def init_db(
    uri: str | Path = DB_FILE,
    erase: bool = False
) -> str:
    '''
    (Re-)initializes the database file.
    If `erase` is True and file exists, erases the old database.

    Returns the uri of the database.
    '''
    db_file = Path(uri)
    db_filename = db_file.name
    if erase and db_file.exists():
        resp = ''
        while resp not in ('y', 'n'):
            usr_in = input(
                f'Are you sure you want to drop all tables from {db_filename}? (Y/[N]) '
            ).strip()

            resp = usr_in.lower()
            if usr_in == '':
                resp = 'n'
            resp = resp[0]

        if resp == 'y':
            logger.debug('Dropping tables from %s', db_filename)
            res = _delete_db(db_file)
            if res:
                logger.debug(
                    'Successfully dropped tables from %s', db_filename)
            else:
                logger.debug(
                    'Failed to drop tables from %s, aborting', db_filename)
                return None
        else:
            logger.debug('Using existing %s', db_filename)

    try:
        with (
            duckdb.connect(db_file) as conn,
            open(
                INIT_FILE,
                mode='r+',
                encoding='utf-8'
            ) as sql_fp
        ):
            sql_script = sql_fp.read()
            conn.sql(sql_script)

        logging.debug('Successfully initialized %s', db_filename)
        return db_file
    except duckdb.Error as e:
        logging.debug('An error occurred: %s', e)
        return None

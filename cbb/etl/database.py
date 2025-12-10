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

import sqlite3
import polars as pl

logger = logging.getLogger(__name__)

SUBMODULE_DIR = Path(__file__).parent
SQL_DIR = SUBMODULE_DIR / 'sql'

DB_DIR = Path.cwd() / 'db'
DB_FILENAME = 'cbb.sqlite'
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
    dummy_spec = ', '.join(f':{col}' for col in df.columns)
    query = (
        f'INSERT INTO {table_spec.name} ({cols_spec}) '
        f'VALUES ({dummy_spec})'
    )

    return query


def _execute_insert_query(
    df: pl.DataFrame,
    conn: sqlite3.Connection,
    query: str
) -> int:
    '''Executes the query on the connection.'''
    try:
        with (
            conn,
            closing(conn.cursor()) as cursor
        ):
            cursor.executemany(query, df.iter_rows(named=True))
            rows = cursor.rowcount
    except sqlite3.Error as e:
        logger.debug('An error occurred during query execution: %s', e)
        rows = -1

    return rows


def insert_to_db(
    df: pl.DataFrame,
    table: Table,
    conn: sqlite3.Connection,
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
                f'\t{col}=excluded.{col}'
                for col in df.columns
                if col not in table_spec.primary_key
            )
            query = (
                f'{insert_query}\n'
                f'ON CONFLICT ({pk_str}) DO UPDATE SET\n'
                f'{upsert_set_spec};'
            )
        case WriteAction.UPDATE:
            update_set_spec = ', '.join(
                f'{col} = :{col}'
                for col in df.columns
                if col not in table_spec.primary_key
            )
            update_pk_spec = ' AND '.join(
                f'{col} = :{col}'
                for col in df.columns
                if col in table_spec.primary_key
            )
            query = (
                f'UPDATE {table_spec.name}\n'
                f'SET {update_set_spec}\n'
                f'WHERE {update_pk_spec}\n;'
            )
        case _:
            query = (
                f'{insert_query}\n'
                f'ON CONFLICT ({pk_str}) DO NOTHING;'
            )

    rows = _execute_insert_query(df, conn, query)

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
            '%s %d rows in %s',
            write_action_str,
            rows,
            table_spec.name
        )

    return rows


def inserts_to_db(
    items: list[tuple[pl.DataFrame, Table, WriteAction]],
    conn: sqlite3.Connection,
) -> list[int]:
    '''
    Inserts multiple DataFrames into the specified tables.
    Returns the number of affected rows.
    '''
    # TODO: find a database package to do concurrent writes
    rows = []
    for df, table, on_conflict in items:
        rows.append(
            insert_to_db(
                df, table, conn,
                write_action=on_conflict
            )
        )

    results = zip(items, rows)
    failed_tables = [res[0][1].value.name for res in results if res[1] == -1]
    if len(failed_tables) > 0:
        logger.debug(
            f'Failed to insert to the following tables: {failed_tables}')

    success_rows = sum(x for x in rows if x >= 0)
    logger.debug(f'Affected {success_rows} rows')

    return [x if x >= 0 else -1 for x in rows]


def _delete_db(
    conn: sqlite3.Connection
) -> bool:
    # TODO: detect fail and do proper rollback
    with (
        conn,
        closing(conn.cursor()) as cursor
    ):
        cursor.execute(
            'SELECT name '
            'FROM sqlite_master '
            "WHERE type=\'table\' "
            "AND name NOT LIKE \'sqlite_%\';"
        )
        tables = [x[0] for x in cursor.fetchall()]
        for table in tables:
            cursor.execute(f'DROP TABLE IF EXISTS {table};')

    return True


def init_db(
    conn: sqlite3.Connection,
    erase: bool = False
) -> bool:
    '''
    (Re-)initializes the database file.
    If `erase` is True and file exists, erases the old DB.
    '''
    if erase and DB_FILE.exists():
        resp = ''
        while resp not in ('y', 'n'):
            usr_in = input(
                f'Are you sure you want to drop all tables from {DB_FILENAME}? (Y/[N]) '
            ).strip()

            resp = usr_in.lower()
            if usr_in == '':
                resp = 'n'
            resp = resp[0]

        if resp == 'y':
            logger.debug('Dropping tables from %s', DB_FILENAME)
            res = _delete_db(conn)
            if res:
                logger.debug(
                    'Successfully dropped tables from %s', DB_FILENAME)
            else:
                logger.debug(
                    'Failed to drop tables from %s, aborting', DB_FILENAME)
                return False
        else:
            logger.debug('Using existing %s', DB_FILENAME)

    try:
        with (
            conn,
            closing(conn.cursor()) as cursor,
            open(
                SQL_DIR / 'create_tables.sql',
                mode='r+',
                encoding='utf-8'
            ) as sql_fp
        ):
            sql_script = sql_fp.read()
            cursor.executescript(sql_script)

        logging.debug('Successfully initialized %s', DB_FILENAME)
        return True
    # TODO: ensure proper rollback procedure
    except sqlite3.OperationalError as e:
        logging.debug('An error occurred: %s', e)
        return False


if __name__ == '__main__':
    main()

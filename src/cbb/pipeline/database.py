"""
This module provides functions and classes for database
operations within the parent module.
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Iterable, Sequence, TypeVar

import duckdb
import polars as pl

from cbb.pipeline.helpers import is_timed_out
from cbb.pipeline.interfaces import AbstractBatchIngestor, AbstractBatchWriter

logger = logging.getLogger(__name__)

SUBMODULE_DIR = Path(__file__).parent
SQL_DIR = SUBMODULE_DIR / 'sql'
INIT_DB_FILENAME = 'create_tables.sql'
INIT_DB_FILE = SQL_DIR / INIT_DB_FILENAME
INIT_DOCUMENT_STORE_FILENAME = 'create_document_store.sql'
INIT_DOCUMENT_STORE_FILE = SQL_DIR / INIT_DOCUMENT_STORE_FILENAME

DB_DIR = Path('C:/Users/olive/iaa/side/cbbv2') / 'data'
DB_FILENAME = 'cbb.duckdb'
DB_FILE = DB_DIR / DB_FILENAME
DOCUMENT_STORE_FILENAME = 'document_store.duckdb'
DOCUMENT_STORE_FILE = DB_DIR / DOCUMENT_STORE_FILENAME

T = TypeVar('T')


@dataclass(frozen=True)
class _TableSpec:
    """Represents the specification for a table."""
    name: str
    primary_key: Sequence[str]


class Table(Enum):
    """
    Abstraction for a SQL table.
    Includes table name and primary key.
    """
    GAMES = _TableSpec('Games', ['id'])
    GAME_STATUSES = _TableSpec('GameStatuses', ['id'])
    VENUES = _TableSpec('Venues', ['id'])
    TEAMS = _TableSpec('Teams', ['id'])
    CONFERENCE_ALIGNMENTS = _TableSpec(
        'ConferenceAlignments', ['team_id', 'conference_id', 'season']
    )
    CONFERENCES = _TableSpec('Conferences', ['id'])
    PLAYS = _TableSpec('Plays', ['game_id', 'sequence_id'])
    PLAY_TYPES = _TableSpec('PlayTypes', ['id'])
    PLAYERS = _TableSpec('Players', ['id'])
    PLAYER_SEASONS = _TableSpec(
        'PlayerSeasons', ['player_id', 'team_id', 'season']
    )
    GAME_LOGS = _TableSpec('GameLogs', ['player_id', 'game_id'])
    PLAYER_BOX_SCORES = _TableSpec('PlayerBoxScores', ['player_id', 'game_id'])
    TEAM_BOX_SCORES = _TableSpec('TeamBoxScores', ['team_id', 'game_id'])


class WriteAction(Enum):
    INSERT = auto()
    UPDATE = auto()
    UPSERT = auto()


def _get_insert_query(
    df: pl.DataFrame,
    table_spec: _TableSpec,
) -> str:
    """Returns the template insert query based on the DataFrame."""
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
    """Returns the update query based on the DataFrame."""
    set_columns = [
        col
        for col in df.columns
        if col not in table_spec.primary_key
    ]
    if len(set_columns) == 0:
        return ''
    update_set_spec = ', '.join(
        f'{col} = df.{col}'
        for col in set_columns
    )
    update_pk_spec = ' AND '.join(
        f'{table_spec.name}.{col} = df.{col}'
        for col in table_spec.primary_key
    )

    # for now, we avoid re-updating tables since DuckDB
    # implicitly deletes them, causing issues whenever
    # foreign keys are present. updates are only used when
    # there is no existing value
    query = (
        f'UPDATE {table_spec.name}\n'
        f'SET {update_set_spec}\n'
        f'FROM df\n'
        f'WHERE {update_pk_spec}\n'
        f'AND {table_spec.name}.{set_columns[0]} IS NULL;'
    )

    return query


def _execute_write_query(
    df: pl.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    query: str
) -> int:
    """Executes the query on the connection."""
    try:
        conn.register('df', df)
        res = conn.execute(query)
        rows = res.fetchall()[0][0]
    except duckdb.Error as e:
        logger.debug('An error occurred during query execution: %s', e)
        rows = -1

    return rows


def _filter_null_primary_key(
    df: pl.DataFrame,
    table_spec: _TableSpec
) -> pl.DataFrame:
    """Sanitizes the DataFrame of any null primary key rows."""
    if df.height == 0:
        return df
    return (
        df
        .filter(
            pl.all_horizontal(
                pl.col(table_spec.primary_key).is_not_null()
            )
        )
    )


def write_db(
    df: pl.DataFrame,
    table: Table,
    conn: duckdb.DuckDBPyConnection,
    write_action: WriteAction = WriteAction.INSERT,
    trace: bool = False
) -> int:
    """
    Performs a write action from the rows of the DataFrame into the table.
    Uses the names of the columns in the DataFrame to construct the query.
    """
    table_spec = table.value
    insert_query = _get_insert_query(df, table_spec)
    pk_str = ', '.join(table_spec.primary_key)

    if write_action == WriteAction.UPSERT:
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
    elif write_action == WriteAction.UPDATE:
        query = _get_update_query(df, table_spec)
        if query == '':
            logger.info('No rows to update, skipping')
            return 0
    else:
        query = (
            f'{insert_query}\n'
            f'ON CONFLICT ({pk_str}) DO NOTHING;'
        )

    df = df.pipe(_filter_null_primary_key, table_spec)
    rows = _execute_write_query(df, conn, query)

    if rows == -1:
        logger.debug('Failed to insert rows to %s', table_spec.name)
    elif trace:
        if write_action == WriteAction.UPSERT:
            write_action_str = 'Upserted'
        elif write_action == WriteAction.UPDATE:
            write_action_str = 'Updated'
        else:
            write_action_str = 'Inserted'

        logger.debug(
            '%s %d rows (of %d) in %s',
            write_action_str,
            rows,
            df.height,
            table_spec.name
        )

    return rows


def get_affected_rows(rows: list[int] | int):
    if isinstance(rows, int):
        return max(rows, -1)
    return sum(r for r in rows if r >= 0)


@dataclass(frozen=True)
class DBWriteDestination:
    table: Table
    write_action: WriteAction


@dataclass(frozen=True)
class DBWriteTask:
    df: pl.DataFrame
    write_destination: DBWriteDestination


@dataclass(frozen=True)
class DBWriteTaskResult:
    task: DBWriteTask
    rows: int


def get_write_tasks(*configs: tuple[pl.DataFrame, Table, WriteAction]):
    return [
        DBWriteTask(
            config[0],
            DBWriteDestination(config[1], config[2])
        )
        for config in configs
    ]


class TransformedWriter(AbstractBatchWriter[Iterable[DBWriteTask], None]):
    DEFAULT_BATCH_ROW_COUNT = 100_000

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[Iterable[DBWriteTask] | None],
        conn: duckdb.DuckDBPyConnection,
        batch_row_count: int = DEFAULT_BATCH_ROW_COUNT,
        flush_timeout: int = AbstractBatchIngestor.DEFAULT_FLUSH_TIMEOUT,
    ):
        super().__init__(
            name, queue, conn,
            batch_size=-1,
            flush_timeout=flush_timeout
        )
        self.batch_row_count = batch_row_count
        self.buffer: dict[DBWriteDestination, pl.DataFrame] = {}

    def _process_batch(self, items: list[DBWriteTask]) -> list[None]:
        tasks = [
            DBWriteTask(df, write_destination)
            for write_destination, df in self.buffer.items()
        ]

        writes_db(
            tasks,
            self.conn
        )

        return []

    def _extend_tasks(
        self,
        tasks: list[DBWriteTask],
    ):
        for task in tasks:
            buffered_rows = self.buffer.get(task.write_destination, None)
            if buffered_rows is None:
                extended_rows = task.df
            else:
                extended_rows = pl.concat([buffered_rows, task.df], how='diagonal_relaxed')
            self.buffer[task.write_destination] = extended_rows

    @property
    def _buffer_row_count(self) -> int:
        rows = sum(
            df.height
            for df in self.buffer.values()
        )
        
        return rows

    @property
    def batch_ready(self) -> bool:
        return self._buffer_row_count >= self.batch_row_count

    async def run(self):
        """Runs the processor."""
        last_flush = time.monotonic()
        n_records = 0
        logger.debug('[%s] Starting processor...', self.name)
        while True:
            tasks = await self.queue.get()
            if tasks is None:
                logger.debug('[%s] Reached sentinel after %d item(s), exiting', self.name, n_records)
                await self.flush()
                await self.end_successors()
                self.queue.task_done()
                break

            n_records += 1
            self._extend_tasks(tasks)
            if (
                self.batch_ready
                or is_timed_out(last_flush, self.flush_timeout)
            ):
                processed_items = await self.flush()
                last_flush = time.monotonic()
                await self.notify_successors(processed_items)

            self.queue.task_done()


def writes_db(
    tasks: list[DBWriteTask],
    conn: duckdb.DuckDBPyConnection,
) -> list[int]:
    """
    Inserts multiple DataFrames into the specified tables.
    Returns the number of affected rows.
    """
    logger.debug('Performing %d write tasks...', len(tasks))
    conn.execute('BEGIN TRANSACTION')
    try:
        # TODO: can probably do better error handling here
        results = [
            DBWriteTaskResult(
                task,
                write_db(
                    task.df, task.write_destination.table, conn,
                    write_action=task.write_destination.write_action,
                )
            )
            for task in tasks
        ]
        conn.execute('COMMIT')
    except Exception as e:
        conn.execute('ROLLBACK')
        raise e

    failed_tables = [
        res.task.write_destination.table.name
        for res in results
        if res.rows == -1
    ]
    if len(failed_tables) > 0:
        logger.debug(
            'Failed write tasks to the following tables: %s', failed_tables
        )

    rows = [result.rows for result in results]
    success_rows = get_affected_rows(rows)
    logger.debug('Affected %s rows', success_rows)

    return rows


def _delete_db(db_file: Path) -> bool:
    """Deletes the existing database file."""
    try:
        db_file.unlink()
        return True
    except PermissionError:
        logger.debug('Another process is using %s, aborting', db_file.name)
        return False


def _prompt_yn(
    prompt: str,
    default: bool | None = False,
) -> bool:
    """
    Get a yes/no answer from the user with retries.

    Args:
        prompt (str): The message to display to the user. Will be suffixed with (Y/N) and the default option.
        default (bool, optional): The default answer, as a boolean (Yes => True, No => False, None => no default). Defaults to False.
    """
    if default is None:
        options = '(Y/N)'
    elif default:
        options = '([Y]/N)'
    else:
        options = '(Y/[N])'

    answer = None
    while answer is None:
        usr_in = input(
            f'{prompt} {options} '
        ).strip()

        resp = usr_in.lower()
        if usr_in == '' and default is not None:
            answer = default
        elif resp[0] in ('y', 'n'):
            answer = resp[0] == 'y'
    return answer


def init_db(
    uri: str | Path = DB_FILE,
    erase: bool = False
) -> Path | None:
    """
    (Re-)initializes the database file.
    If `erase` is True and file exists, erases the old database.

    Returns:
         The uri of the database or None if not successful.
    """
    db_file = Path(uri)
    db_filename = db_file.name
    if erase and db_file.exists():
        yes = _prompt_yn(
            f'Are you sure you want to delete {db_filename}?',
            default=False
        )

        if yes:
            logger.debug('Deleting %s', db_filename)
            res = _delete_db(db_file)
            if res:
                logger.debug(
                    'Successfully deleted %s', db_filename
                )
            else:
                logger.debug(
                    'Failed to delete %s, aborting', db_filename
                )
                return None
        else:
            logger.debug('Using existing %s', db_filename)

    try:
        with (
            duckdb.connect(db_file) as conn,
            open(
                INIT_DB_FILE,
                mode='r+',
                encoding='utf-8'
            ) as sql_fp
        ):
            sql_script = sql_fp.read()
            conn.sql(sql_script)

        logging.debug('Successfully initialized %s', db_filename)
        return db_file
    except duckdb.Error as e:
        logging.critical('An error occurred when connecting to the database: %s', e)
        return None


def init_document_store(
    uri: str | Path = DOCUMENT_STORE_FILE,
    erase: bool = False
) -> Path | None:
    """
    (Re-)initializes the document store file.
    If `erase` is True and file exists, erases the old document store.

    Returns:
        The uri of the document store or None if not successful.
    """
    store_file = Path(uri)
    store_filename = store_file.name
    if erase and store_file.exists():
        yes = _prompt_yn(
            f'Are you sure you want to delete {store_filename}?',
            default=False
        )

        if yes:
            logger.debug('Deleting %s', store_filename)
            res = _delete_db(store_file)
            if res:
                logger.debug(
                    'Successfully deleted %s', store_filename
                )
            else:
                logger.debug(
                    'Failed to delete %s, aborting', store_filename
                )
                return None
        else:
            logger.debug('Using existing %s', store_filename)

    try:
        with (
            duckdb.connect(store_file) as conn,
            open(
                INIT_DOCUMENT_STORE_FILE,
                mode='r+',
                encoding='utf-8'
            ) as sql_fp
        ):
            sql_script = sql_fp.read()
            conn.sql(sql_script)

        logging.debug('Successfully initialized %s', store_filename)
        return store_file
    except duckdb.Error as e:
        logging.critical('An error occurred when connecting to the document store: %s', e)
        return None

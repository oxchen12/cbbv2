"""
This module provides functions for scraping data from ESPN.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import batched
from pathlib import Path
from typing import Any, TypeVar, Callable, Awaitable, Iterable
import asyncio
import datetime as dt
import json
import logging
import math
import re
import time

from bs4 import BeautifulSoup
import aiohttp
import backoff
import duckdb
import polars as pl
import tqdm
import tqdm.asyncio

from .date import (
    validate_season,
    get_season_start,
    CALENDAR_DT_FORMAT, get_season,
)
from ._helpers import (
    deep_get,
    deep_pop,
    JSONPayload
)

logger = logging.getLogger(__name__)
exclude_loggers = ('_log_backoff', '_log_giveup')
for logger_name in exclude_loggers:
    logging.getLogger(logger_name).disabled = True
T = TypeVar('T')

# TODO: implement switching to women's, perhaps with a module manager
# URLs
API_PREFIX = (
    'https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball'
)
GAME_API_TEMPLATE = (
    f'{API_PREFIX}/summary?region=us&lang=en&contentorigin=espn&event={{}}'
)
CONFERENCES_API_URL = f'{API_PREFIX}/scoreboard/conferences?groups=50'

WEB_PREFIX = 'https://www.espn.com/mens-college-basketball'
STANDINGS_TEMPLATE = f'{WEB_PREFIX}/standings/_/season/{{}}'
SCHEDULE_TEMPLATE = f'{WEB_PREFIX}/schedule/_/date/{{}}'
PLAYER_TEMPLATE = f'{WEB_PREFIX}/player/_/id/{{}}'

# request parameters
DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}

# dates
SCHEDULE_DATE_FORMAT = '%Y%m%d'

# extraction
EXTRACT_DESTINATION = Path('./data/raw')
MAX_QUEUE_SIZE = 500


def is_non_transient(code: int) -> bool:
    return 400 <= code < 500


def _is_giveup_http(e: aiohttp.ClientResponseError) -> bool:
    return is_non_transient(e.status)


def _extract_json(text: str) -> JSONPayload:
    soup = BeautifulSoup(text, 'html.parser')
    html_raw = ''
    for x in soup.find_all('script'):
        if str(x).startswith('<script>window'):
            html_raw = str(x).removeprefix(
                '<script>'
            ).removesuffix('</script>')
            break

    if html_raw == '':
        logger.debug('no script tags found, continuing')
        return {}

    # script consists of assignments to properties of `window`
    matches = re.split(r'window\[.*?\]=', html_raw)
    if len(matches) < 3:
        logging.debug('no json data found, continuing')
        return {}
    data_match = matches[2].replace(';', '')
    json_raw = json.loads(data_match)

    return json_raw


def _validate_season(season: int | str):
    try:
        season = int(season)
    except ValueError as e:
        raise ValueError(f'season must be integer-like (got {season})') from e

    validate_season(season)


def get_game_url(game_id: int | str) -> str:
    """Get the url for the given game_id."""
    return GAME_API_TEMPLATE.format(game_id)


def get_standings_url(season: int | str) -> str:
    """Get the url for the given season's standings."""
    return STANDINGS_TEMPLATE.format(season)


def get_schedule_url(date: dt.date):
    """Get the url for the given date's schedule."""
    date_str = date.strftime(SCHEDULE_DATE_FORMAT)
    return SCHEDULE_TEMPLATE.format(date_str)


def get_player_url(player_id: int | str) -> str:
    """Get the url for the given player_id."""
    return PLAYER_TEMPLATE.format(player_id)


class AsyncClient:
    """Provides an interface for async scraping."""

    MIN_MAX_CONCURRENTS = 1
    MAX_MAX_CONCURRENTS = 20
    DEFAULT_MAX_CONCURRENTS = 10

    SLEEP_TIME = 0.1

    def __init__(self, max_concurrents: int = DEFAULT_MAX_CONCURRENTS):
        if (
            max_concurrents < AsyncClient.MIN_MAX_CONCURRENTS
            or max_concurrents > AsyncClient.MAX_MAX_CONCURRENTS
        ):
            raise ValueError(
                f'`max_concurrents` must be between {AsyncClient.MIN_MAX_CONCURRENTS} and {AsyncClient.MAX_MAX_CONCURRENTS}'
            )
        self._semaphore = asyncio.Semaphore(max_concurrents)
        self._session = None

    async def __aenter__(self) -> AsyncClient:
        if self._session is None:
            logger.debug('Creating new session')
            self._session = aiohttp.ClientSession(
                headers=DEFAULT_HEADERS,
                # TODO: error logic
                raise_for_status=True
            )

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            logger.debug('Exiting session')
            await self._session.close()
            self._session = None

    def check_session_exists(self) -> None:
        """Asserts whether the session has been initialized."""
        if self._session is None:
            raise RuntimeError(
                'Client session not initialized. Use `async with`.'
            )

    @backoff.on_exception(
        backoff.expo,
        aiohttp.ClientResponseError,
        giveup=_is_giveup_http,
        raise_on_giveup=True,
        max_tries=5,
        max_time=60,
        factor=2,
        logger=None
    )
    async def _fetch(
        self,
        url: str,
        processor: Callable[[aiohttp.ClientResponse], Awaitable[T]]
    ) -> T:
        """
        Get a request from the url and process the
        response using the `processor`.
        """
        self.check_session_exists()
        short_url = (
            url
            .removeprefix(API_PREFIX)
            .removeprefix(WEB_PREFIX)
        )

        logger.debug('Fetching from %s...', short_url)
        async with self._semaphore:
            get_start = time.perf_counter()
            async with self._session.get(url) as resp:
                get_end = time.perf_counter() - get_start
                logger.debug(
                    'Got %d (%.2fs) from %s',
                    resp.status, get_end, short_url
                )

                await asyncio.sleep(AsyncClient.SLEEP_TIME)
                return await processor(resp)

    async def _extract_json_from_html(self, url: str) -> JSONPayload:
        """Extract json from HTML page."""

        async def process_json_from_html(resp: aiohttp.ClientResponse) -> JSONPayload:
            text = await resp.text(encoding='utf-8')
            return _extract_json(text)

        return await self._fetch(url, process_json_from_html)

    async def _extract_as_json(self, url: str) -> JSONPayload:
        """Extract json from json page."""

        async def process_json(resp: aiohttp.ClientResponse) -> JSONPayload:
            return await resp.json()

        return await self._fetch(url, process_json)

    async def get_raw_game_json(
        self,
        game_id: int | str
    ) -> JSONPayload:
        """Get the raw json from the game page."""
        url = get_game_url(game_id)
        return await self._extract_as_json(url)

    async def get_raw_standings_json(
        self,
        season: int | str
    ) -> JSONPayload:
        """Get the raw json from the standings page for a season."""
        _validate_season(season)
        url = get_standings_url(season)
        return await self._extract_json_from_html(url)

    async def get_raw_schedule_json(
        self,
        date: dt.date
    ) -> JSONPayload:
        """
        Get the raw json from the schedule page for a given date.
        A date `str` should be formatted as SCHEDULE_DATE_FORMAT.
        """
        url = get_schedule_url(date)
        return await self._extract_json_from_html(url)

    async def get_raw_player_json(
        self,
        player_id: int | str
    ) -> JSONPayload:
        """Get the raw json from the player page."""
        url = get_player_url(player_id)
        return await self._extract_json_from_html(url)


@dataclass(frozen=True)
class Record:
    key: str
    up_to_date: bool
    payload: JSONPayload
    error: bool


class AbstractBatchProcessor[T](ABC):
    """
    Extracts information from in-memory queued records for later use.

    Attributes:
        T: the queue input type.
    """
    DEFAULT_BATCH_SIZE = 300
    DEFAULT_FLUSH_TIMEOUT = 120  # time between flushes (sec)
    DEFAULT_TIMEOUT = 600  # time between receiving new records (sec)

    def __init__(
        self,
        queue: asyncio.Queue[T],
        name: str,
        batch_size: int = DEFAULT_BATCH_SIZE
    ):
        self.queue = queue
        self.name = name
        self.buffer: list[T] = []
        self.batch_size = batch_size
        self.successors: list[AbstractBatchProcessor[T]] = []

    def add_successor(self, successor: AbstractBatchProcessor[T]):
        """Adds a successor to the processor."""
        logger.debug('[%s] Adding successor [%s]', self.name, successor.name)
        self.successors.append(successor)

    async def notify_successors(self, batch: list[T]):
        logger.debug('[%s] Notifying sucessors...', self.name)

        async def _notify_successor(successor: AbstractBatchProcessor[T], batch: list[T]):
            for item in batch:
                await successor.queue.put(item)

        async with asyncio.TaskGroup() as tg:
            for successor in self.successors:
                tg.create_task(_notify_successor(successor, batch))

    async def end_successors(self):
        logger.debug('[%s] Ending successors...', self.name)

        async def end_successor(successor: AbstractBatchProcessor[T]):
            await successor.queue.put(None)

        for successor in self.successors:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(end_successor(successor))

    async def flush(self):
        """Flushes the buffer and pushes records to successors."""
        if (
            len(self.buffer) == 0
            or len(self.successors) == 1 and self.buffer[0] is None
        ):
            return

        buffer_snapshot = self.buffer.copy()

        logger.debug('[%s] Flushing buffer...', self.name)
        await self._process()
        self.buffer.clear()

        # logger.debug('[%s] Notifying successors...', self.name)
        await self.notify_successors(buffer_snapshot)

    @abstractmethod
    async def _process(self):
        """Processes flushed records. Should handle sentinel."""
        raise NotImplementedError('Child classes must implement this method.')

    async def run(self):
        record = {}
        last_flush = time.monotonic()
        last_record = time.monotonic()
        n_records = 0
        logger.debug('[%s] Starting processor...', self.name)
        while record is not None:
            # if time.monotonic() - last_record > self.DEFAULT_TIMEOUT:
            #     logger.debug('')
            #     record = None
            #     continue
            record = await self.queue.get()
            last_record = time.monotonic()
            if record is None:
                logger.debug('[%s] Reached sentinel after %d record(s), exiting', self.name, n_records)
                await self.flush()
                await self.end_successors()
                self.queue.task_done()
                continue

            n_records += 1
            self.buffer.append(record)
            if (
                len(self.buffer) >= self.batch_size
                or time.monotonic() - last_flush > self.DEFAULT_FLUSH_TIMEOUT
            ):
                last_flush = time.monotonic()
                await self.flush()

            self.queue.task_done()


class RecordBatchWriter(AbstractBatchProcessor[Record]):
    """
    Writes raw document store records to a file.

    Attributes:
        name (str): The name of the writer, used to identify record source.
        queue (asyncio.Queue): The queue to receive records from.
        batch_size (int): The number of records in a write batch
    """

    def __init__(
        self,
        queue: asyncio.Queue[Record],
        conn: duckdb.DuckDBPyConnection,
        name: str,
        batch_size: int = AbstractBatchProcessor.DEFAULT_BATCH_SIZE
    ):
        """
        Args:
            conn (duckdb.DuckDBPyConnection): The connection to the duckDB destination database.
            name (str): The name of the writer, used to identify record source.
            batch_size (int): The number of records in a write batch.
        """
        super().__init__(queue, name, batch_size=batch_size)
        self.conn = conn

    async def _process(self):
        raw_batch = [
            {
                'key': record.key,
                'name': self.name,
                'up_to_date': record.up_to_date,
                'timestamp': dt.datetime.now(),
                'payload': json.dumps(record.payload)
            }
            for record in self.buffer
            if record is not None and not record.error
        ]
        if len(raw_batch) == 0:
            logger.debug('[%s] No valid records in buffer.', self.name)
            return
        batch = pl.from_dicts(raw_batch)
        res = self.conn.execute(
            'INSERT INTO Documents (key, name, timestamp, up_to_date, payload)\n'
            'SELECT key, name, timestamp, up_to_date, payload FROM batch\n'
        )
        rows = res.fetchall()[0][0]
        if rows < 0:
            logger.debug('[%s] An error occurred while writing to document store.')
        else:
            logger.debug('[%s] Wrote %d record(s) to document store.', self.name, rows)


def update_discovery_manifest(
    conn: duckdb.DuckDBPyConnection,
    keys: Sequence[T],
    name: str,
) -> int:
    batch = pl.DataFrame(
        {
            'key': keys,
            'name': [name] * len(keys),
        }
    )
    res = conn.execute(
        'INSERT INTO DiscoveryManifest (key, name)\n'
        'SELECT key, name FROM batch\n'
        'ON CONFLICT DO NOTHING'
    )
    rows = res.fetchall()[0][0]
    return rows


class RecordBatchGameIDExtractor(AbstractBatchProcessor[Record]):
    """
    Extracts game IDs from schedule payloads.
    """

    def __init__(
        self,
        queue: asyncio.Queue[Record],
        conn: duckdb.DuckDBPyConnection,
        batch_size: int = AbstractBatchProcessor.DEFAULT_BATCH_SIZE
    ):
        super().__init__(queue, 'game_id_extractor', batch_size=batch_size)
        self.conn = conn

    def _get_buffer_game_ids(self):
        game_ids: list[str] = []
        for record in self.buffer:
            if record is None:
                continue
            schedule = deep_get(
                record.payload,
                'page', 'content', 'events',
                default={}
            )
            for events in schedule.values():
                for event in events:
                    game_id = event.get('id')
                    if game_id is not None:
                        game_ids.append(game_id)
        return game_ids

    async def _process(self):
        game_ids = self._get_buffer_game_ids()

        if len(game_ids) == 0:
            logger.debug('[%s] No game ids in buffer.', self.name)
            return

        rows_written = update_discovery_manifest(
            self.conn,
            game_ids,
            'game',
        )

        if rows_written < 0:
            logger.debug('[%s] Something went wrong when writing game IDs', self.name)
        else:
            logger.debug('[%s] Wrote %d signature(s) to discovery manifest.', self.name, rows_written)


class RecordBatchPlayerIDExtractor(AbstractBatchProcessor[Record]):
    """
    Extracts player IDs from game payloads.
    """

    def __init__(
        self,
        queue: asyncio.Queue[Record],
        conn: duckdb.DuckDBPyConnection,
        batch_size: int = AbstractBatchProcessor.DEFAULT_BATCH_SIZE
    ):
        super().__init__(queue, 'player_id_extractor', batch_size=batch_size)
        self.conn = conn

    def _get_buffer_player_ids(self):
        player_ids: list[str] = []
        for record in self.buffer:
            if record is None:
                continue
            boxes = deep_get(
                record.payload,
                'boxscore', 'players',
                default=[]
            )
            for box in boxes:
                stats = box.get('statistics', [])
                if len(stats) == 0:
                    continue
                athletes = stats[0].get('athletes', [])
                for ath in athletes:
                    player_id = deep_get(
                        ath,
                        'athlete', 'id',
                    )
                    if player_id is not None:
                        player_ids.append(player_id)
        return player_ids

    async def _process(self):
        player_ids = self._get_buffer_player_ids()

        if len(player_ids) == 0:
            logger.debug('[%s] No player ids in buffer.', self.name)
            return

        rows_written = update_discovery_manifest(
            self.conn,
            player_ids,
            'player',
        )

        if rows_written < 0:
            logger.debug('[%s] Something went wrong when writing player IDs', self.name)
        else:
            logger.debug('[%s] Wrote %d signature(s) to discovery manifest.', self.name, rows_written)


async def _batch_extract_to_queue(
    queue: asyncio.Queue,
    name: str,
    keys: Iterable[T],
    fetch_func: Callable[[T], Awaitable[JSONPayload]],
    key_to_str_func: Callable[[T], str],
    up_to_date_func: Callable[[T], bool],
    cull_func: Callable[[JSONPayload], JSONPayload] = lambda payload: payload,
    batch_size: int = RecordBatchWriter.DEFAULT_BATCH_SIZE,
    disable_tqdm: bool = False,
):
    """
    Perform asynchronous batch extraction of records to a queue using the supplied pattern.

    Args:
        queue (AsyncQueue): Write queue to push results to.
        name (str): The name of the extract job.
        keys (Iterable[T]): The keys to extract records for.
        fetch_func (Callable[[T], Awaitable[JSONObject]]): The function to fetch the payload using the key.
        key_to_str_func (Callable[[T], str]): The function to convert the key to string.
        up_to_date_func (Callable[[T], bool]): The function to deduce whether the key's record is up to date.
        cull_func (Callable[[JSONPayload], JSONPayload]): The function to cull unneeded keys from the payload.
        batch_size (int): Size of the batches.
        disable_tqdm (bool): Whether to disable tqdm progress bars.
    """
    # Guarantee that no duplicates exist
    keys = set(keys)

    async def fetch_record_and_put(key: T):
        try:
            payload = await fetch_func(key)
            payload = cull_func(payload)
        except aiohttp.ClientError as e:
            payload = None
        record = Record(
            # TODO: generalize these to take the payload instead
            key=key_to_str_func(key),
            up_to_date=up_to_date_func(key),
            payload=payload,
            error=payload is None
        )
        # logger.debug('Queue size: %d', queue.qsize())
        await queue.put(record)

    for batch in tqdm.tqdm(
        batched(keys, batch_size),
        total=math.ceil(len(keys) / batch_size),
        desc=f'[{name}] Extract batches',
        position=0,
        leave=True,
        disable=disable_tqdm
    ):
        tasks = [
            fetch_record_and_put(key)
            for key in batch
        ]
        await tqdm.asyncio.tqdm.gather(
            *tasks,
            total=len(tasks),
            desc=f'[{name}] Batch records',
            position=1,
            leave=False,
            disable=disable_tqdm
        )


def _cull_keys(keys: Iterable[str | Sequence[str]]) -> Callable[[JSONPayload], JSONPayload]:
    """Creates a cull function to remove the supplied keys.
    If iterable is provided as a key, search for the nested key to pop."""

    def _cull_payload_keys(payload: JSONPayload) -> JSONPayload:
        for key in keys:
            if isinstance(key, str):
                key = [key]
            deep_pop(payload, *key, default=None)
        return payload

    return _cull_payload_keys


# extractors
async def extract_standings(
    client: AsyncClient,
    queue: asyncio.Queue,
    seasons: Iterable[int]
):
    """
    Extracts standings for the given season range.

    Args:
        client (AsyncClient): HTTP client for requesting data.
        queue (asyncio.Queue): Write queue to push results to.
        seasons (Iterable[int]): The seasons to extract standings for.
    """
    logger.debug('Extracting standings...')
    # TODO: Check which keys are 1) already stored and 2) up to date
    await _batch_extract_to_queue(
        queue,
        seasons,
        client.get_raw_standings_json,
        lambda season: str(season),
        lambda season: get_season(dt.date.today()) > season
    )
    logger.debug('Finished extracting standings.')

    # TODO: return value?


async def extract_schedules_seasons(
    client: AsyncClient,
    queue: asyncio.Queue,
    seasons: Iterable[int]
):
    """
    Extracts schedules for the given season range.

    Args:
        client (AsyncClient): HTTP client for requesting data.
        queue (asyncio.Queue): Write queue to push results to.
        seasons (Iterable[int]): The seasons to extract schedules for.
    """
    logger.debug('Getting representative dates...')
    rep_dates = await _get_rep_dates_seasons(client, seasons)

    logger.debug('Extracting schedules...')
    # TODO: Check which keys are 1) already stored and 2) up to date
    await _batch_extract_to_queue(
        queue,
        rep_dates,
        client.get_raw_schedule_json,
        lambda date: date.strftime('%Y-%m-%d'),
        lambda date: dt.date.today() > date
    )
    logger.debug('Finished extracting schedules.')

    # TODO: return value?


async def extract_lane(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    name: str,
    extractor: Callable[[AsyncClient, asyncio.Queue, ...], Awaitable[Any]],
    params: dict[str, Any],
    max_queue_size: int = MAX_QUEUE_SIZE,
):
    """
    Creates an extraction/batch writing lane for the given
    extraction task.

    Args:
        conn (duckdb.DuckDBPyConnection): Connection to the document store.
        client (AsyncClient): HTTP client for requesting data.
        name (str): Document store table name.
        extractor (Callable[[AsyncClient, asyncio.Queue, ...], Awaitable[Any])): Extraction function that puts results in queue.
        params (dict[str, Any]): Extraction parameters.
        max_queue_size (int): Max queue size.
    """
    queue = asyncio.Queue(maxsize=max_queue_size)
    writer = RecordBatchWriter(conn, name, queue)
    writer_task = asyncio.create_task(writer.run())

    # TODO: figure out how to filter out keys from here
    #       or pass this information to the extractor
    await extractor(client, queue, **params)

    # Sentinel
    await queue.put(None)
    await writer_task

    # TODO: return value?


def init_document_store(conn: duckdb.DuckDBPyConnection):
    """Initialize the document store."""
    conn.execute(
        'CREATE TABLE IF NOT EXISTS Documents (\n'
        '   key VARCHAR PRIMARY KEY,\n'
        '   name VARCHAR,\n'
        '   up_to_date BOOLEAN,\n'
        '   payload JSON,\n'
        ')'
    )


def get_up_to_date_keys(
    conn: duckdb.DuckDBPyConnection,
    name: str
) -> list[str]:
    # TODO: anti-injection
    res = conn.sql(
        'SELECT key\n'
        'FROM Documents\n'
        f'WHERE name = {name} AND up_to_date\n'
    )
    return [
        row[0]
        for row in res.fetchall()
    ]


async def extract_all(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    seasons: Iterable[int],
):
    """
    Orchestrates extraction for all channels of raw data.

    Args:
        conn (duckdb.DuckDBPyConnection): Connection to the document store.
        client (AsyncClient): HTTP client for requesting data.
        seasons (Iterable[int]): The seasons to extract data for.
    """
    init_document_store(conn)
    # TODO: extract standings
    await extract_lane(
        conn,
        client,
        'standings',
        extract_standings_seasons,
        {'seasons': seasons}
    )

    # TODO: extract schedules
    await extract_lane(
        conn,
        client,
        'schedule',
        extract_schedules_seasons,
        {'seasons': seasons}
    )

    # TODO: deduce game_id's from schedules

    # TODO: extract games

    # TODO: deduce player_id's from games

    # TODO: extract players
# representative date helpers
def _create_rep_date_range(
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
    # minimize pages to search by accessing schedules from adjacent dates
    rep_dates = [
        date
        for i, date in enumerate(calendar)
        if i % 3 == 1 or i == len(calendar) - 1
    ]

    return rep_dates


async def _get_rep_dates_json(init_schedule: JSONPayload) -> list[dt.date]:
    """Get the representative dates from the raw initial schedule."""
    season_json = init_schedule['page']['content']['season']
    # the calendar field for some reason doesn't get every date
    # so instead, we manually generate all dates from start to end
    rep_dates = _create_rep_date_range(
        season_json['startDate'], season_json['endDate']
    )

    return rep_dates


async def get_rep_dates_seasons(
    client: AsyncClient,
    seasons: Iterable[int]
) -> list[dt.date]:
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
    rep_dates = [
        date
        for srd in season_rep_dates
        for date in srd
    ]

    return rep_dates

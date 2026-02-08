"""
This module provides functions for scraping data from ESPN.
"""
import asyncio
import datetime as dt
import json
import logging
import math
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from itertools import batched
from pathlib import Path
from typing import (Any, Awaitable, Callable, Iterable, Sequence, Type, TypeVar)

import aiohttp
import backoff
import tqdm
import tqdm.asyncio
from bs4 import BeautifulSoup

import cbb.pipeline._helpers
from .date import (get_season, validate_season)
from .interfaces import (AbstractBatchIngestor, AbstractImmediateIngestor, AbstractIngestor)

logger = logging.getLogger(__name__)
exclude_loggers = ('_log_backoff', '_log_giveup')
for logger_name in exclude_loggers:
    logging.getLogger(logger_name).disabled = True
T = TypeVar('T')

# TODO: implement switching to women's, perhaps with a module manager
# URLs
LEAGUE = 'mens-college-basketball'
API_PREFIX = (
    f'https://site.web.api.espn.com/apis/site/v2/sports/basketball/{LEAGUE}'
)
GAME_API_TEMPLATE = (
    f'{API_PREFIX}/summary?region=us&lang=en&contentorigin=espn&event={{}}'
)
CONFERENCES_API_URL = f'{API_PREFIX}/scoreboard/conferences?groups=50'

WEB_PREFIX = f'https://www.espn.com/{LEAGUE}'
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
KEY_DATE_FORMAT = '%Y-%m-%d'

# extraction
EXTRACT_DESTINATION = Path('./data/raw')
MAX_QUEUE_SIZE = 500


def is_non_transient(code: int) -> bool:
    return 400 <= code < 500


def _is_giveup_http(e: Exception) -> bool:
    if not isinstance(e, aiohttp.ClientResponseError):
        return False
    return is_non_transient(e.status)


def _extract_json(text: str) -> cbb.pipeline._helpers.JSONPayload:
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
        self._session: aiohttp.ClientSession | None = None

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
            async with self._session.get(url) as resp:  # type: ignore[union-attr]
                get_end = time.perf_counter() - get_start
                logger.debug(
                    'Got %d (%.2fs) from %s',
                    resp.status, get_end, short_url
                )

                await asyncio.sleep(AsyncClient.SLEEP_TIME)
                return await processor(resp)

    async def _extract_json_from_html(self, url: str) -> cbb.pipeline._helpers.JSONPayload:
        """Extract json from HTML page."""

        async def process_json_from_html(resp: aiohttp.ClientResponse) -> cbb.pipeline._helpers.JSONPayload:
            text = await resp.text(encoding='utf-8')
            return _extract_json(text)

        return await self._fetch(url, process_json_from_html)

    async def _extract_as_json(self, url: str) -> cbb.pipeline._helpers.JSONPayload:
        """Extract json from json page."""

        async def process_json(resp: aiohttp.ClientResponse) -> cbb.pipeline._helpers.JSONPayload:
            return await resp.json()

        return await self._fetch(url, process_json)

    async def get_raw_game_json(
        self,
        game_id: int | str
    ) -> cbb.pipeline._helpers.JSONPayload:
        """Get the raw json from the game page."""
        url = get_game_url(game_id)
        return await self._extract_as_json(url)

    async def get_raw_standings_json(
        self,
        season: int | str
    ) -> cbb.pipeline._helpers.JSONPayload:
        # TODO: see if I can refactor these so they're not all separate
        """Get the raw json from the standings page for a season."""
        _validate_season(season)
        url = get_standings_url(season)
        return await self._extract_json_from_html(url)

    async def get_raw_schedule_json(
        self,
        date: dt.date
    ) -> cbb.pipeline._helpers.JSONPayload:
        """
        Get the raw json from the schedule page for a given date.
        A date `str` should be formatted as SCHEDULE_DATE_FORMAT.
        """
        url = get_schedule_url(date)
        return await self._extract_json_from_html(url)

    async def get_raw_player_json(
        self,
        player_id: int | str
    ) -> cbb.pipeline._helpers.JSONPayload:
        """Get the raw json from the player page."""
        url = get_player_url(player_id)
        return await self._extract_json_from_html(url)


class RecordType(Enum):
    STANDINGS = ('standings', int)
    SCHEDULE = ('schedule', dt.date)
    GAME = ('game', int)
    PLAYER = ('player', int)

    def __init__(self, label: str, key_type: Type):
        self.label = label
        self.key_type = key_type

    def raw_key_to_str(self, raw_key: Any):
        # TODO: I feel like I can redo this class with generic typing
        match self.key_type:
            case dt.date:
                return raw_key.strftime(KEY_DATE_FORMAT)
            case _:
                return str(raw_key)


@dataclass(frozen=True)
class Record:
    type_: RecordType
    payload: cbb.pipeline._helpers.JSONPayload
    error: bool


@dataclass(frozen=True)
class IncompleteRecord[T](Record):
    raw_key: T


@dataclass(frozen=True)
class CompleteRecord(Record):
    key: str
    up_to_date: bool


class RecordIngestor(AbstractImmediateIngestor[Record, Record]):
    """Transient extract producer to push records to multiple successors."""

    def _process_item(self, record: Record) -> list[Record]:
        return [record]


class AbstractRecordCompleter[T](AbstractImmediateIngestor[IncompleteRecord, CompleteRecord], ABC):
    """
    Completes input records with a specified raw key type and pushes to successors.

    Attributes:
        T: The type of the input raw keys.
    """

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[IncompleteRecord | None],
        type_: RecordType
    ):
        super().__init__(name, queue)
        self.type_ = type_

    def _process_item(self, record: IncompleteRecord) -> list[CompleteRecord]:
        completed = self.complete_record(record)
        if completed is None:
            return []
        return [completed]

    def complete_record(self, incomplete_record: IncompleteRecord[T]) -> CompleteRecord | None:
        if incomplete_record.type_ != self.type_:
            return None
        return CompleteRecord(
            type_=incomplete_record.type_,
            payload=incomplete_record.payload,
            error=incomplete_record.error,
            key=self._complete_key(incomplete_record),
            up_to_date=self._complete_up_to_date(incomplete_record),
        )

    @staticmethod
    def _complete_key(incomplete_record: IncompleteRecord) -> str:
        return (
            incomplete_record.type_
            .raw_key_to_str(incomplete_record.raw_key)
        )

    @staticmethod
    @abstractmethod
    def _complete_up_to_date(incomplete_record: IncompleteRecord) -> bool:
        raise NotImplementedError('Child classes must implement this method.')


class StandingsRecordCompleter(AbstractRecordCompleter[int]):
    """Completes standings records."""

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[IncompleteRecord | None],
    ):
        super().__init__(name, queue, RecordType.STANDINGS)

    @staticmethod
    def _complete_key(incomplete_record: IncompleteRecord[int]) -> str:
        return str(incomplete_record.raw_key)

    @staticmethod
    def _complete_up_to_date(incomplete_record: IncompleteRecord[int]) -> bool:
        return incomplete_record.raw_key < get_season(dt.date.today())


class ScheduleRecordCompleter(AbstractRecordCompleter[dt.date]):
    """Completes schedule records."""

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[IncompleteRecord | None],
    ):
        super().__init__(name, queue, RecordType.SCHEDULE)

    @staticmethod
    def _complete_up_to_date(incomplete_record: IncompleteRecord[dt.date]) -> bool:
        return incomplete_record.raw_key < dt.date.today()


class GameRecordCompleter(AbstractRecordCompleter[int]):
    """Completes game records."""

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[IncompleteRecord | None],
    ):
        super().__init__(name, queue, RecordType.GAME)

    @staticmethod
    def _complete_up_to_date(incomplete_record: IncompleteRecord[int]) -> bool:
        game_date = GameRecordCompleter._get_game_date(incomplete_record.payload)
        if game_date is None:
            # TODO: double-check; I think this is right because
            #       new competitions should report their dates
            return True
        return game_date < dt.date.today()

    @staticmethod
    def _get_game_date(payload: cbb.pipeline._helpers.JSONPayload) -> dt.date | None:
        competitions = cbb.pipeline._helpers.deep_get(
            payload,
            'header', 'competitions',
            default=None
        )
        if len(competitions) is None:
            return None
        raw_date = competitions[0].get('date')
        if raw_date is None:
            return None
        date = dt.date.strptime(raw_date, '%Y-%m-%dT%H:%M%z')
        return date


class PlayerRecordCompleter(AbstractRecordCompleter[int]):
    """Completes player records."""

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[IncompleteRecord | None]
    ):
        super().__init__(name, queue, RecordType.PLAYER)

    @staticmethod
    def _complete_up_to_date(incomplete_record: IncompleteRecord[int]) -> bool:
        return True


class GameIDExtractor(AbstractImmediateIngestor[Record, IncompleteRecord]):
    """Extracts game IDs from schedule page JSONs."""

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[Record | None]
    ):
        super().__init__(name, queue)
        self.seen_game_ids: set[int] = set()

    def _process_item(self, item: Record) -> list[IncompleteRecord]:
        if item is None:
            return []
        game_ids: list[int] = []
        schedule = cbb.pipeline._helpers.deep_get(
            item.payload,
            'page', 'content', 'events',
            default={}
        )
        for events in schedule.values():
            for event in events:
                game_id = event.get('id')
                try:
                    game_id = int(game_id)
                    game_ids.append(game_id)
                except (ValueError, TypeError):
                    pass
        new_game_ids = set(game_ids).difference(self.seen_game_ids)
        self.seen_game_ids.update(new_game_ids)
        return [
            IncompleteRecord(
                type_=RecordType.GAME,
                payload={},
                error=False,
                raw_key=game_id,
            )
            for game_id in new_game_ids
        ]


class PlayerIDExtractor(AbstractImmediateIngestor[Record, IncompleteRecord]):
    """Extracts player IDs from game page JSONs."""

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[Record | None]
    ):
        super().__init__(name, queue)
        self.seen_player_ids: set[int] = set()

    def _process_item(self, item: Record) -> list[IncompleteRecord]:
        if item is None:
            return []
        player_ids: list[int] = []
        # TODO: refactor this into a helper or helpers for later transformation
        boxes = cbb.pipeline._helpers.deep_get(
            item.payload,
            'boxscore', 'players',
            default=[]
        )
        for box in boxes:
            stats = box.get('statistics', [])
            if len(stats) == 0:
                continue
            athletes = stats[0].get('athletes', [])
            for ath in athletes:
                player_id = cbb.pipeline._helpers.deep_get(
                    ath,
                    'athlete', 'id',
                )
                try:
                    player_id = int(player_id)
                    player_ids.append(player_id)
                except (ValueError, TypeError):
                    pass
        new_player_ids = set(player_ids).difference(self.seen_player_ids)
        self.seen_player_ids.update(new_player_ids)
        return [
            IncompleteRecord(
                type_=RecordType.PLAYER,
                payload={},
                error=False,
                raw_key=player_id,
            )
            for player_id in new_player_ids
        ]


# extractor logic
async def _batch_extract_to_queue(
    queue: asyncio.Queue[IncompleteRecord[T]],
    record_type: RecordType,
    keys: Iterable[T],
    fetch_func: Callable[[T], Awaitable[cbb.pipeline._helpers.JSONPayload]],
    name: str | None = None,
    # TODO: technically, this function can do any transformation (but culling is a subset and I will leave it for now)
    cull_func: Callable[[cbb.pipeline._helpers.JSONPayload], cbb.pipeline._helpers.JSONPayload] = lambda
        payload: payload,
    batch_size: int = AbstractBatchIngestor.DEFAULT_BATCH_SIZE,
    disable_tqdm: bool = False,
):
    """
    Perform asynchronous batch extraction of records to a queue using the supplied pattern.

    Args:
        queue (asyncio.Queue[IncompleteRecord[T]]): The queue to push results to.
        record_type (RecordType): The type of record to extract.
        keys (Iterable[T]): The keys to extract records for.
        fetch_func (Callable[[T], Awaitable[JSONObject]]): The function to fetch the payload using the key.
        name (str, optional): The name of the batch extraction task. Defaults to the record type label.
        cull_func (Callable[[JSONPayload], JSONPayload], optional): The function to cull unneeded payload keys from the payload. Defaults to transient.
        batch_size (int, optional): Size of the batches. Defaults to 300.
        disable_tqdm (bool, optional): Whether to disable tqdm progress bars. Defaults to False.
    """
    # guarantee that no duplicates exist
    keys = set(keys)
    if name is None:
        name = record_type.label

    # arg_type = get_args(T)
    # logger.info(arg_type)
    # if record_type.key_type != T:
    #     raise ValueError(f'[{name}] Keys must match record key type (expected {record_type.key_type}, got {T})')

    async def fetch_record_and_put(key: T):
        try:
            payload = await fetch_func(key)
            payload = cull_func(payload)
        except aiohttp.ClientError as e:
            logger.debug('[%s] Client encountered an error: %s', name, e)
            payload = {}

        record = IncompleteRecord(
            type_=record_type,
            payload=payload,
            error=payload is None,
            raw_key=key
        )
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
        res = await cbb.pipeline._helpers.tqdm_gather(
            *tasks,
            total=len(tasks),
            desc=f'[{name}] Extraction batch records',
            position=1,
            leave=False,
            disable=disable_tqdm,
            return_exceptions=True,
        )
        logger.debug(
            '[%s] %d tasks errored out', name, len([result for result in res if isinstance(result, Exception)])
        )


def _cull_keys(keys: Iterable[str | Sequence[str]]) -> Callable[
    [cbb.pipeline._helpers.JSONPayload], cbb.pipeline._helpers.JSONPayload]:
    """Creates a cull function to remove the supplied keys.

    Args:
        keys (Iterable[str | Sequence[str]]): List of keys to extract. Pass nested keys as Sequence.
    """

    def _cull_payload_keys(payload: cbb.pipeline._helpers.JSONPayload) -> cbb.pipeline._helpers.JSONPayload:
        for key in keys:
            if isinstance(key, str):
                key = [key]
            cbb.pipeline._helpers.deep_pop(payload, *key, default=None)
        return payload

    return _cull_payload_keys


async def extract_standings(
    client: AsyncClient,
    queue: asyncio.Queue,
    seasons: Iterable[int],
    existing_seasons: Iterable[int] | None = None
):
    """
    Extracts standings for the given seasons.

    Args:
        client (AsyncClient): HTTP client for requesting data.
        queue (asyncio.Queue): Write queue to push results to.
        seasons (Iterable[int]): The seasons to extract standings for.
        existing_seasons (Iterable[int], optional): Existing seasons to exclude when extracting standings.
    """
    if existing_seasons is None:
        existing_seasons = []

    seasons = set(seasons).difference(existing_seasons)

    logger.debug('Extracting standings...')
    await _batch_extract_to_queue(
        queue,
        RecordType.STANDINGS,
        seasons,
        client.get_raw_standings_json,
    )
    logger.debug('Finished extracting standings.')

    # TODO: return value?


async def extract_schedules_seasons(
    client: AsyncClient,
    queue: asyncio.Queue,
    seasons: Iterable[int],
    existing_dates: Iterable[dt.date] | None = None,
):
    """
    Extracts schedules for the given season range.

    Args:
        client (AsyncClient): HTTP client for requesting data.
        queue (asyncio.Queue): Write queue to push results to.
        seasons (Iterable[int]): The seasons to extract schedules for.
        existing_dates (Iterable[dt.date], optional): Existing dates to exclude when extracting schedules.
    """

    logger.debug('Getting representative dates...')
    rep_dates = await cbb.pipeline._helpers.get_rep_dates_seasons(client, seasons)

    await extract_schedules(client, queue, rep_dates, existing_dates)

    # TODO: return value?


async def extract_schedules(
    client: AsyncClient,
    queue: asyncio.Queue,
    dates: Iterable[dt.date],
    existing_dates: Iterable[dt.date] | None = None,
):
    """
    Extracts schedules for the given dates. Note that schedules
    are stored with the previous and next days as well.

    Args:
        client (AsyncClient): HTTP client for requesting data.
        queue (asyncio.Queue): Write queue to push results to.
        dates (Iterable[dt.date]): The dates to extract schedules for.
        existing_dates (Iterable[dt.date], optional): Existing dates to exclude when extracting schedules.
    """
    if existing_dates is None:
        existing_dates = []

    dates = set(dates).difference(existing_dates)

    logger.debug('Extracting schedules...')
    await _batch_extract_to_queue(
        queue,
        RecordType.SCHEDULE,
        dates,
        client.get_raw_schedule_json,
    )
    logger.debug('Finished extracting schedules.')


async def extract_games(
    client: AsyncClient,
    queue: asyncio.Queue,
    game_ids: Iterable[int],
    existing_game_ids: Iterable[int] | None = None,
):
    """
    Extracts game info for the given game IDs.

    Args:
        client (AsyncClient): HTTP client for requesting data.
        queue (asyncio.Queue): Write queue to push results to.
        game_ids (Iterable[int]): The game IDs to extract info for.
        existing_game_ids (Iterable[int], optional): Existing game IDs to exclude when extracting games.
    """
    if existing_game_ids is None:
        existing_game_ids = []

    game_ids = set(game_ids).difference(existing_game_ids)
    keys_to_cull = ('news', 'videos', 'standings')

    logger.debug('Extracting games...')
    await _batch_extract_to_queue(
        queue,
        RecordType.GAME,
        game_ids,
        client.get_raw_game_json,
        cull_func=_cull_keys(keys_to_cull)
    )
    logger.debug('Finished extracting games.')


async def extract_players(
    client: AsyncClient,
    queue: asyncio.Queue,
    player_ids: Iterable[int],
    existing_player_ids: Iterable[int] | None = None,
):
    """
    Extracts player info for the given player IDs.

    Args:
        client (AsyncClient): HTTP client for requesting data.
        queue (asyncio.Queue): Write queue to push results to.
        player_ids (Iterable[int]): The players to extract info for.
        existing_player_ids (Iterable[int], optional): Existing player IDs to exclude when extracting players.
    """
    if existing_player_ids is None:
        existing_player_ids = []

    player_ids = set(player_ids).difference(existing_player_ids)
    keys_to_cull = (
        'app', 'ads',
        ('page', 'content', 'teams'),
        ('page', 'content', 'navigation'),
        ('page', 'content', 'stndngs'),
    )
    logger.debug('Extracting players...')
    await _batch_extract_to_queue(
        queue,
        RecordType.PLAYER,
        player_ids,
        client.get_raw_player_json,
        cull_func=_cull_keys(keys_to_cull)
    )
    logger.debug('Finished extracting players.')


# abstracted extraction
async def extract_lane(
    queue: asyncio.Queue,
    client: AsyncClient,
    record_type: RecordType,
    extractor: Callable[[AsyncClient, asyncio.Queue, ...], Awaitable[Any]],
    params: dict[str, Any] | None = None,
    name: str | None = None,
    successors: Iterable[AbstractIngestor[Record, Any]] | None = None,
):
    """
    Creates an extraction/batch writing lane for the given
    extraction task.

    Args:
        queue (asyncio.Queue): Queue to push results to.
        client (AsyncClient): HTTP client for requesting data.
        record_type (RecordType): The type of record to extract.
        extractor (Callable[[AsyncClient, asyncio.Queue, ...], Awaitable[Any])): Extraction function that puts results in queue.
        params (dict[str, Any]): Extraction parameters.
        name (str, optional): Name of the extraction task. Defaults to the record type label.
        successors (Iterable[AbstractExtractProducer[Record]], optional): Batch writer successors.
    """
    if name is None:
        name = record_type.label
    if successors is None:
        successors = []
    ingestor = RecordIngestor(name, queue)
    for successor in successors:
        ingestor.add_successor(successor)
    writer_task = asyncio.create_task(ingestor.run())

    if params is None:
        params = {}
    await extractor(client, queue, **params)

    # sentinel
    await queue.put(None)
    await writer_task

    # TODO: return value?

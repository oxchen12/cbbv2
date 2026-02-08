#!python
"""Entry point for pipeline."""
import asyncio
import datetime as dt
import logging.config
import sys
from pathlib import Path
from typing import Iterable

import duckdb

from cbb.pipeline.helpers import get_rep_dates_seasons

sys.path.insert(0, 'C:/Users/olive/iaa/side/cbbv2')

from cbb.pipeline.database import init_document_store
from cbb.pipeline.date import MAX_SEASON, MIN_SEASON, get_season_range
from cbb.pipeline.extract import (
    AsyncClient, CompleteRecord, GameIDExtractor, GameRecordCompleter, IncompleteRecord, KEY_DATE_FORMAT,
    PlayerIDExtractor, PlayerRecordCompleter, Record,
    RecordIngestor, RecordType,
    ScheduleRecordCompleter, StandingsRecordCompleter, extract_games, extract_players,
    extract_schedules, extract_standings
)
from cbb.pipeline.load import DiscoveryBatchLoader, DocumentBatchLoader

LOG_DIR = Path('C:/Users/olive/iaa/side/cbbv2/log')
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOGGING_LEVEL = 'DEBUG'
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '{funcName} ({levelname}) {message}',
            'style': '{'
        },
        'formal': {
            'format': '[{asctime}] {name} / {funcName} ({levelname}) {message}',
            'style': '{'
        }
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'formal',
            'filename': str(LOG_DIR / f'pipeline_orchestrator.log'),
            'mode': 'w+'
        },
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        '': {
            'handlers': ['file', 'console'],
            'level': LOGGING_LEVEL,
            'propagate': True
        }
    }
}

logger = logging.getLogger(__name__)
# exclude_loggers = ('_log_backoff', '_log_giveup')
# for name in exclude_loggers:
#     logging.getLogger(name).disabled = True
logging.config.dictConfig(LOGGING_CONFIG)

QUEUE_MAX_SIZE = 500

def get_existing_keys(
    conn: duckdb.DuckDBPyConnection,
    record_type: RecordType,
) -> list[str]:
    res = conn.sql(
        'SELECT key\n'
        'FROM Documents\n'
        'WHERE record_type = $record_type AND COALESCE(up_to_date, FALSE)\n',
        params={'record_type': record_type.label}
    )
    # TODO: make this a generator? since I can just
    #       pass iterables to my extractors
    return [
        row[0]
        for row in res.fetchall()
    ]


def get_discovered_keys(
    conn: duckdb.DuckDBPyConnection,
    record_type: RecordType,
) -> list[str]:
    res = conn.sql(
        'SELECT key\n'
        'FROM DiscoveryManifest\n'
        'WHERE record_type = $record_type\n'
        'EXCEPT\n'
        'SELECT key FROM Documents WHERE record_type = $record_type AND COALESCE(up_to_date, FALSE)',
        params={'record_type': record_type.label}
    )
    return [
        row[0]
        for row in res.fetchall()
    ]

async def _orchestrate(
    conn: duckdb.DuckDBPyConnection,
    client: AsyncClient,
    seasons: Iterable[int],
    queue_max_size: int = QUEUE_MAX_SIZE
):
    # TODO: setup loaders
    discovery_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    discovery_loader = DiscoveryBatchLoader(
        'discovery_loader',
        discovery_queue,
        conn
    )
    discovery_loader_task = asyncio.create_task(discovery_loader.run())
    documents_queue: asyncio.Queue[CompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    documents_loader = DocumentBatchLoader(
        'documents_loader',
        documents_queue,
        conn
    )
    document_loader_task = asyncio.create_task(documents_loader.run())

    # TODO: setup record completer for standings
    standings_completer_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    standings_completer = StandingsRecordCompleter(
        'standings_completer',
        standings_completer_queue,
    )
    standings_completer.add_successor(documents_loader)
    standings_completer_task = asyncio.create_task(standings_completer.run())

    # TODO: setup ingestor for standings
    standings_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    standings_ingestor = RecordIngestor(
        'standings',
        standings_queue,  # type: ignore[arg-type]
    )
    standings_ingestor.add_successor(standings_completer)  # type: ignore[arg-type]
    standings_ingestor_task = asyncio.create_task(standings_ingestor.run())

    # TODO: run extract lane for standings
    existing_seasons = [
        int(season)
        for season in get_existing_keys(conn, RecordType.STANDINGS)
    ]
    await extract_standings(
        client,
        standings_queue,
        seasons,
        existing_seasons=existing_seasons
    )
    await standings_queue.put(None)
    await standings_completer_queue.put(None)

    # TODO: setup record completer for schedules
    schedule_completer_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    schedule_completer = ScheduleRecordCompleter(
        'schedule_completer',
        schedule_completer_queue
    )
    schedule_completer.add_successor(documents_loader)
    schedule_completer_task = asyncio.create_task(schedule_completer.run())

    # TODO: setup game ID extractor for schedules
    game_id_extractor_queue: asyncio.Queue[Record | None] = asyncio.Queue(maxsize=queue_max_size)
    game_id_extractor = GameIDExtractor(
        'game_id_extractor',
        game_id_extractor_queue,
    )
    game_id_extractor.add_successor(discovery_loader)
    game_id_extractor_task = asyncio.create_task(game_id_extractor.run())

    # TODO: setup ingestor for schedules
    schedules_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    schedules_ingestor = RecordIngestor(
        'schedule',
        schedules_queue,  # type: ignore[arg-type]
    )
    schedules_ingestor.add_successor(schedule_completer)  # type: ignore[arg-type]
    schedules_ingestor.add_successor(game_id_extractor)
    schedules_ingestor_task = asyncio.create_task(schedules_ingestor.run())

    # TODO: setup extract lane for schedules
    dates = await get_rep_dates_seasons(client, seasons)
    existing_dates = [
        dt.date.strptime(date, KEY_DATE_FORMAT)
        for date in get_existing_keys(conn, RecordType.SCHEDULE)
    ]
    await extract_schedules(
        client,
        schedules_queue,
        dates,
        existing_dates=existing_dates
    )
    await schedules_queue.put(None)
    await schedule_completer_queue.put(None)
    await game_id_extractor_queue.put(None)

    # TODO: cleanup standings and schedules tasks
    await standings_ingestor_task
    await standings_completer_task
    await schedules_ingestor_task
    await schedule_completer_task
    await game_id_extractor_task
    # TODO: manually flush game IDs
    await discovery_loader.flush()

    # TODO: setup record completer for game
    game_completer_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    game_completer = GameRecordCompleter(
        'game_completer',
        game_completer_queue,
    )
    game_completer.add_successor(documents_loader)
    game_completer_task = asyncio.create_task(game_completer.run())

    # TODO: setup player ID extractor for game
    player_id_extractor_queue: asyncio.Queue[Record | None] = asyncio.Queue(maxsize=queue_max_size)
    player_id_extractor = PlayerIDExtractor(
        'player_id_extractor',
        player_id_extractor_queue,
    )
    player_id_extractor.add_successor(discovery_loader)
    player_id_extractor_task = asyncio.create_task(player_id_extractor.run())

    # TODO: setup ingestor for games
    discovered_games = [
        int(game_id)
        for game_id in get_discovered_keys(conn, RecordType.GAME)
    ]
    existing_games = [
        int(game_id)
        for game_id in get_existing_keys(conn, RecordType.GAME)
    ]
    games_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    game_ingestor = RecordIngestor(
        'game',
        games_queue  # type: ignore[arg-type]
    )
    game_ingestor.add_successor(game_completer)  # type: ignore[arg-type]
    game_ingestor.add_successor(player_id_extractor)
    game_ingestor_task = asyncio.create_task(game_ingestor.run())

    # TODO: setup extract lane for games
    await extract_games(
        client,
        games_queue,
        discovered_games,
        existing_game_ids=existing_games
    )
    await games_queue.put(None)
    await game_completer_queue.put(None)
    await player_id_extractor_queue.put(None)

    # TODO: cleanup game tasks
    await game_ingestor_task
    await game_completer_task
    await player_id_extractor_task
    # TODO: manually flush player IDs
    await discovery_loader.flush()

    # TODO: setup record completer for player
    player_completer_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    player_completer = PlayerRecordCompleter(
        'player_completer',
        player_completer_queue,
    )
    player_completer.add_successor(documents_loader)
    player_completer_task = asyncio.create_task(player_completer.run())

    # TODO: setup ingestor for player
    players_queue: asyncio.Queue[IncompleteRecord | None] = asyncio.Queue(maxsize=queue_max_size)
    player_ingestor = RecordIngestor(
        'player',
        players_queue  # type: ignore[arg-type]
    )
    player_ingestor.add_successor(player_completer)  # type: ignore[arg-type]
    player_ingestor_task = asyncio.create_task(player_ingestor.run())

    # TODO: setup extract lane for players
    discovered_players = [
        int(player_id)
        for player_id in get_discovered_keys(conn, RecordType.PLAYER)
    ]
    existing_players = [
        int(player_id)
        for player_id in get_existing_keys(conn, RecordType.PLAYER)
    ]
    await extract_players(
        client,
        players_queue,
        discovered_players,
        existing_player_ids=existing_players
    )
    await players_queue.put(None)
    await player_completer_queue.put(None)

    # TODO: cleanup player tasks
    await player_ingestor_task
    await player_completer_task

    # TODO: wait for loaders to finish
    await discovery_queue.put(None)
    await documents_queue.put(None)

    await discovery_loader_task
    await document_loader_task

    # TODO: handle TRANSFORM


async def orchestrate(
    start_season: int = MIN_SEASON,
    end_season: int = MAX_SEASON,
    erase: bool = False
):
    # TODO: initialize document store
    uri = init_document_store(erase=erase)
    assert uri is not None

    with duckdb.connect(uri) as conn:
        async with AsyncClient() as client:
            seasons = get_season_range(start_season, end_season)
            await _orchestrate(conn, client, seasons)


if __name__ == '__main__':
    asyncio.run(
        orchestrate(
            end_season=2025,
            erase=False
        )
    )

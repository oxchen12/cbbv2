"""
This module provides functions for loading transformed
data from ESPN into a local SQLite database.
"""
import asyncio
import datetime as dt
import json
import logging

import duckdb
import polars as pl

from .extract import (
    CompleteRecord, IncompleteRecord
)
from .interfaces import AbstractBatchIngestor

logger = logging.getLogger(__name__)


class DiscoveryBatchLoader(AbstractBatchIngestor[IncompleteRecord, None]):
    """Loads discovered keys into the discovery manifest."""

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[IncompleteRecord | None],
        conn: duckdb.DuckDBPyConnection,
        batch_size: int = AbstractBatchIngestor.DEFAULT_BATCH_SIZE,
        flush_timeout: int = AbstractBatchIngestor.DEFAULT_FLUSH_TIMEOUT
    ):
        """
        Args:
            name (str): The name of the loader.
            queue (asyncio.Queue): The queue to pull records from.
            conn (duckdb.DuckDBPyConnection): The connection to the document store database.
            batch_size (int): The number of items to process in each batch.
            flush_timeout (int): The maximum time between batch flushes. Unused if batch_process is False.
        """
        super().__init__(name, queue, batch_size=batch_size, flush_timeout=flush_timeout)
        self.conn = conn

    def _process_batch(self, records: list[IncompleteRecord]) -> list[None]:
        """Loads discovered keys into the discovery manifest."""
        records = [record for record in records if record is not None]
        if len(records) == 0:
            return []

        batch_table = pl.from_dicts(
            [
                {
                    'key': record.raw_key,
                    'record_type': record.type_.label,
                }
                for record in records
            ],
            strict=False,
            schema={
                'key': pl.String,
                'record_type': pl.String,
            }
        )

        res = self.conn.execute(
            'INSERT INTO DiscoveryManifest (key, record_type)\n'
            'SELECT key, record_type\n'
            'FROM batch_table\n'
            'ON CONFLICT DO NOTHING'
        )
        rows = res.fetchall()[0][0]
        if rows < 0:
            logger.debug('[%s] Something went wrong when inserting to discovery manifest', self.name)
        else:
            logger.debug('[%s] Wrote %d rows to discovery manifest', self.name, rows)

        return []


class DocumentBatchLoader(AbstractBatchIngestor[CompleteRecord, None]):
    """Loads extracted records into the document store."""

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[CompleteRecord | None],
        conn: duckdb.DuckDBPyConnection,
        batch_size: int = AbstractBatchIngestor.DEFAULT_BATCH_SIZE,
        flush_timeout: int = AbstractBatchIngestor.DEFAULT_FLUSH_TIMEOUT
    ):
        """
        Args:
            name (str): The name of the loader.
            queue (asyncio.Queue): The queue to pull records from.
            conn (duckdb.DuckDBPyConnection): The connection to the document store database.
            batch_size (int): The number of items to process in each batch.
            flush_timeout (int): The maximum time between batch flushes. Unused if batch_process is False.
        """
        super().__init__(name, queue, batch_size=batch_size, flush_timeout=flush_timeout)
        self.conn = conn

    def _process_batch(self, records: list[CompleteRecord]) -> list[None]:
        """Loads extracted records into the document store."""
        records = [record for record in records if record is not None]
        if len(records) == 0:
            return []

        batch_table = pl.from_dicts(
            [
                {
                    'key': str(record.key),
                    'record_type': record.type_.label,
                    'timestamp': dt.datetime.now(),
                    'up_to_date': record.up_to_date,
                    'payload': json.dumps(record.payload)
                }
                for record in records
            ]
        )

        res = self.conn.execute(
            'INSERT INTO Documents (key, record_type, timestamp, up_to_date, payload)\n'
            'SELECT key, record_type, timestamp, up_to_date, payload\n'
            'FROM batch_table'
        )
        rows = res.fetchall()[0][0]
        if rows < 0:
            logger.debug('[%s] Something went wrong when inserting to document store', self.name)
        else:
            logger.debug('[%s] Wrote %d rows to document store', self.name, rows)

        return []

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
from .interfaces import AbstractBatchIngestor, AbstractBatchWriter

logger = logging.getLogger(__name__)


class DiscoveryBatchLoader(AbstractBatchWriter[IncompleteRecord, None]):
    """Loads discovered keys into the discovery manifest."""

    def _process_batch(self, records: list[IncompleteRecord]) -> list[None]:
        """Loads discovered keys into the discovery manifest."""
        records = [record for record in records if record is not None]
        if len(records) == 0:
            return []

        batch_table = pl.from_dicts(
            [
                {
                    'document_key': record.raw_key,
                    'record_type': record.type_.label,
                }
                for record in records
            ],
            strict=False,
            schema={
                'document_key': pl.String,
                'record_type': pl.String,
            }
        )

        res = self.conn.execute(
            'INSERT INTO DiscoveryManifest (document_key, record_type)\n'
            'SELECT document_key, record_type\n'
            'FROM batch_table\n'
            'ON CONFLICT DO NOTHING'
        )
        rows = res.fetchall()[0][0]
        if rows < 0:
            logger.debug('[%s] Something went wrong when inserting to discovery manifest', self.name)
        else:
            logger.debug('[%s] Wrote %d rows to discovery manifest', self.name, rows)

        return []


class DocumentBatchLoader(AbstractBatchWriter[CompleteRecord, None]):
    """Loads extracted records into the document store."""

    def _process_batch(self, records: list[CompleteRecord]) -> list[None]:
        """Loads extracted records into the document store."""
        records = [record for record in records if record is not None]
        if len(records) == 0:
            return []

        batch_table = pl.from_dicts(
            [
                {
                    'document_key': str(record.document_key),
                    'record_type': record.type_.label,
                    'timestamp': dt.datetime.now(),
                    'up_to_date': record.up_to_date,
                    'payload': json.dumps(record.payload)
                }
                for record in records
            ]
        )

        res = self.conn.execute(
            'INSERT INTO Documents (document_key, record_type, timestamp, up_to_date, payload)\n'
            'SELECT document_key, record_type, timestamp, up_to_date, payload\n'
            'FROM batch_table'
        )
        rows = res.fetchall()[0][0]
        if rows < 0:
            logger.debug('[%s] Something went wrong when inserting to document store', self.name)
        else:
            logger.debug('[%s] Wrote %d rows to document store', self.name, rows)

        return []

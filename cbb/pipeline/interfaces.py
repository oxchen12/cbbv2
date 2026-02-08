"""This module defines abstract interfaces."""
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, TypeVar

logger = logging.getLogger(__name__)
T = TypeVar('T')


def _is_timed_out(
    last: float,
    timeout: float | int
) -> bool:
    """Helper to check whether time since last action exceeds timeout.

    Args:
        last (float): The time since last action. Should be calculated using time.monotonic().
        timeout (float): The maximum time between actions.
    """
    return (time.monotonic() - last) > timeout


class AbstractIngestor[I, O](ABC):
    """
    Consumes data from a source, processes it, then produces to any number of
    successor queues.

    Attributes:
        I: the queue input type.
        O: the producer output type.
    """

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[I | None],
        propagate_end: bool = False
    ):
        """
        Args:
            queue (asyncio.Queue): The source queue.
            name (str): The name of this producer.
            propagate_end (bool): Whether to terminate successors upon ending.
        """
        self.queue = queue
        self.name = name
        self.propagate_end = propagate_end
        self.successors: list[AbstractIngestor[O, Any]] = []

    def add_successor(self, successor: AbstractIngestor[O, Any]):
        """Adds a successor to the processor.

        Args:
            successor (AbstractIngestor[O, Any]): The successor to add. Successor input type must match processor output type.
        """
        logger.debug('[%s] Adding successor [%s]', self.name, successor.name)
        self.successors.append(successor)

    async def notify_successors(self, items: list[O]):
        """Pushes new items to successors.

        Args:
            items (list[O]): The new items to push to successors.
        """

        # logger.debug('[%s] Notifying successors...', self.name)

        async def _notify_successor(successor: AbstractIngestor[O, Any]):
            for item in items:
                await successor.queue.put(item)

        async with asyncio.TaskGroup() as tg:
            for successor in self.successors:
                tg.create_task(_notify_successor(successor))

    async def end_successors(self):
        """Sends a sentinel value to successors."""
        if self.propagate_end:
            logger.debug('[%s] Ending successors...', self.name)
            await self.notify_successors([None])

    @abstractmethod
    async def run(self):
        """Runs the processor."""
        raise NotImplementedError('Child classes must implement this method.')


class AbstractImmediateIngestor[I, O](AbstractIngestor[I, O], ABC):
    @abstractmethod
    def _process_item(self, item: I) -> list[O]:
        """Processes an item. Should handle sentinel elegantly.

        Args:
            item (I): The item to process.
        """
        # TODO: decide whether I want to make this functional instead
        raise NotImplementedError('Child classes must implement this method.')

    async def run(self):
        """Runs the processor."""
        n_records = 0
        logger.debug('[%s] Starting processor...', self.name)
        while True:
            item = await self.queue.get()
            if item is None:
                logger.debug('[%s] Reached sentinel after %d item(s), exiting', self.name, n_records)
                await self.end_successors()
                self.queue.task_done()
                break

            n_records += 1
            processed_items = self._process_item(item)
            await self.notify_successors(processed_items)

            self.queue.task_done()


class AbstractBatchIngestor[I, O](AbstractIngestor[I, O], ABC):
    """
    AbstractExtractProducer that processes items in batches.
    Best for I/O-bound tasks.

    Attributes:
        I: the queue input type.
        O: the producer output type.
    """

    DEFAULT_BATCH_SIZE = 300
    DEFAULT_FLUSH_TIMEOUT = 120  # time between flushes (sec)

    def __init__(
        self,
        name: str,
        queue: asyncio.Queue[I | None],
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_timeout: int = DEFAULT_FLUSH_TIMEOUT
    ):
        """
        Args:
            queue (asyncio.Queue): The source queue.
            name (str): The name of this producer.
            batch_size (int): The number of items to process in each batch.
            flush_timeout (int): The maximum time between batch flushes. Unused if batch_process is False.
        """
        super().__init__(name, queue)
        self.batch_size = batch_size
        self.flush_timeout = flush_timeout
        self.buffer: list[I] = []

    @abstractmethod
    def _process_batch(self, items: list[I]) -> list[O]:
        """Applies transformations or other operations on the batch. Should handle sentinel elegantly.

        Args:
            items (list[I]): The items to process.
        """
        # TODO: decide whether I want to make this functional instead
        raise NotImplementedError('Child classes must implement this method.')

    async def flush(self) -> list[O]:
        """Flushes the buffer and returns processed items."""
        buffer_size = len(self.buffer)
        if buffer_size == 0:
            logger.debug('[%s] No items in buffer to flush', self.name)
            return []

        logger.debug('[%s] Flushing buffer...', self.name)
        processed_items = self._process_batch(self.buffer)
        self.buffer.clear()

        logger.debug('[%s] Flushed buffer of size %d', self.name, buffer_size)
        return processed_items

    async def run(self):
        """Runs the processor."""
        last_flush = time.monotonic()
        last_item = time.monotonic()
        n_records = 0
        logger.debug('[%s] Starting processor...', self.name)
        while True:
            if _is_timed_out(last_item, self.timeout):
                logger.debug('[%s] Timed out %d seconds after receiving last item', self.name, self.timeout)
                break
            item = await self.queue.get()
            last_item = time.monotonic()
            if item is None:
                logger.debug('[%s] Reached sentinel after %d item(s), exiting', self.name, n_records)
                await self.flush()
                await self.end_successors()
                self.queue.task_done()
                break

            n_records += 1
            self.buffer.append(item)
            if (
                len(self.buffer) >= self.batch_size
                or _is_timed_out(last_flush, self.flush_timeout)
            ):
                processed_items = await self.flush()
                last_flush = time.monotonic()
                await self.notify_successors(processed_items)

            self.queue.task_done()

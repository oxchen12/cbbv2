'''
This module provides custom asynchronous logic for
internal limiting.
'''
from typing import Any, Callable, Awaitable

import asyncio

from cbb.etl.scrape import AsyncClient

MAX_TRANSFORM_COROUTINES = AsyncClient.DEFAULT_MAX_CONCURRENTS * 5

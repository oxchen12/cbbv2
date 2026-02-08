"""
Miscellaneous Python helpers.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any, Iterable

import polars as pl
from tqdm.asyncio import tqdm

import cbb.pipeline.extract
from cbb.pipeline.date import CALENDAR_DT_FORMAT, get_season_start

JSONPayload = dict[str, Any]


def deep_get(
    d: dict | Any,
    *keys: str,
    default: Any = None
):
    if not isinstance(d, dict):
        return default
    cur = d
    value = default
    for key in keys:
        value = cur.get(key, None)
        if value is None:
            return default
        cur = value
    return value


def deep_pop(
    d: dict | Any,
    *keys: str,
    default: Any = None
):
    if not isinstance(d, dict) or len(keys) == 0:
        return default
    cur = d
    for key in keys[:-1]:
        value = cur.get(key, None)
        if value is None:
            return default
        cur = value
    return cur.pop(keys[-1], None)


def safe_int(s: str) -> int | None:
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


# representative date helpers
def create_rep_date_range(
    start: dt.date,
    end: dt.date
) -> list[dt.date]:
    start_date = max(start, get_season_start(start.year))
    end_date = end
    calendar = pl.date_range(
        start_date,
        end_date,
        interval=dt.timedelta(days=1),
        eager=True
    )
    rep_dates = [
        date
        for i, date in enumerate(calendar)
        if i % 3 == 1 or i == len(calendar) - 1
    ]

    return rep_dates


def _create_rep_date_range_format(
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

    return create_rep_date_range(start_date, end_date)


async def _get_rep_dates_json(init_schedule: JSONPayload) -> list[dt.date]:
    """Get the representative dates from the raw initial schedule."""
    season_json = init_schedule['page']['content']['season']
    # the calendar field for some reason doesn't get every date
    # so instead, we manually generate all dates from start to end
    rep_dates = _create_rep_date_range_format(
        season_json['startDate'], season_json['endDate']
    )

    return rep_dates


async def get_rep_dates_seasons(
    client: cbb.pipeline.extract.AsyncClient,
    seasons: Iterable[int]
) -> list[dt.date]:
    # TODO: see if I can decouple AsyncClient from this
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


async def tqdm_gather(*fs, return_exceptions=False, always_cancel: Iterable[type[Exception]] = None, **kwargs):
    """Custom implementation of tqdm async gather handling task exceptions.

    Taken from: https://github.com/tqdm/tqdm/issues/1286
    """
    if not return_exceptions:
        return await tqdm.gather(*fs, **kwargs)

    if always_cancel is None:
        always_cancel = []

    async def wrap(f):
        try:
            return await f
        except always_cancel as e:
            raise from e
        except Exception as e:
            return e

    return await tqdm.gather(*map(wrap, fs), **kwargs)

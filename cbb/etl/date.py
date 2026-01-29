"""
This module contains utility functions for dealing with
CBB season dates.
"""
import datetime as dt

import polars as pl

def get_season(date: dt.date) -> int:
    return int(date.month > 7) + date.year

def get_season_pl(date: pl.Expr) -> pl.Expr:
    return (
        date
        .dt.year()
        .add(
            date
            .dt.month()
            .gt(7)
            .cast(pl.Int64)
        )
    )

# earliest season for which ESPN has data
MIN_SEASON = 2003
MAX_SEASON = get_season(dt.date.today())

DEFAULT_SEASON_START = dt.date(MAX_SEASON, 11, 1)

CALENDAR_DT_FORMAT = '%Y-%m-%dT%H:%MZ'


class InvalidSeasonError(ValueError):
    """Raised when a season is out of range."""

    def __init__(
        self,
        message: str = f"Season must be between {MIN_SEASON} and {MAX_SEASON}"
    ):
        super().__init__(message)


def validate_season(season: int) -> bool:
    """Validates the season."""
    return MIN_SEASON <= season <= MAX_SEASON


def get_season_start(season: int) -> dt.date:
    """Get the start date of the season."""
    return (
        DEFAULT_SEASON_START
        .replace(year=season-1)
    )

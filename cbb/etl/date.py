'''
This module contains utility functions for dealing with
CBB season dates.
'''
import datetime as dt


def get_season(date: dt.date) -> int:
    return int(date.month < 7) + date.year


# earliest season for which ESPN has data
MIN_SEASON = 2003
MAX_SEASON = get_season(dt.date.today())

DEFAULT_SEASON_START = dt.date(MAX_SEASON, 11, 1)


class InvalidSeasonError(ValueError):
    '''Raised when a season is out of range.'''

    def __init__(
        self,
        message: str = f"Season must be between {MIN_SEASON} and {MAX_SEASON}"
    ):
        super().__init__(message)


def validate_season(season: int) -> bool:
    '''Validates the season.'''
    if not MIN_SEASON <= season <= MAX_SEASON:
        raise InvalidSeasonError()

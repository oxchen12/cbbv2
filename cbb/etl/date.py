'''
This module contains utility functions for dealing with
CBB season dates.
'''


def get_season(date: dt.date) -> int:
    return int(date.month < 7) + date.year

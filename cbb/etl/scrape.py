'''
This module provides functions for scraping data from ESPN.
'''
from typing import Any
import requests

GENDER = 'mens'
GAME_API_TEMPLATE = (
    'https://site.web.api.espn.com/apis/site/v2/sports/basketball/{}-college-basketball/'
    'summary?region=us&lang=en&contentorigin=espn&event={}'
)
TIMEOUT = 30


def _get_raw(url: str,
             timeout: int = TIMEOUT) -> dict[str, Any]:
    '''Get the raw json from the API.'''
    # TODO: retry handling
    res = requests.get(
        url=url,
        timeout=timeout
    )
    if res.status_code != 200:
        # TODO: error handling
        pass

    return res.json()

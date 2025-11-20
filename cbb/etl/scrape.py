'''
This module provides functions for scraping data from ESPN.
'''
from typing import Any
import json
import re

from bs4 import BeautifulSoup
import requests


# url construction
# TODO: make this a module-wide selection, or select through a UI
GENDER = 'mens'
GAME_API_TEMPLATE = (
    'https://site.web.api.espn.com/apis/site/v2/sports/basketball/{}-college-basketball/'
    'summary?region=us&lang=en&contentorigin=espn&event={}'
)
STANDINGS_TEMPLATE = 'https://www.espn.com/{}-college-basketball/standings/_/season/{}'

# request parameters
TIMEOUT = 30
HEADERS = {
    # TODO: I think this is generic enough that there isn't a security risk
    #       but I should make sure
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}


def _get_raw(url: str,
             timeout: int = TIMEOUT,
             headers: dict | None = None) -> requests.Response:
    '''Get the raw json from the API.'''
    if headers is None:
        headers = HEADERS
    # TODO: retry handling
    resp = requests.get(
        url=url,
        timeout=timeout,
        headers=headers
    )
    if resp.status_code != 200:
        # TODO: error handling
        pass

    return resp


def get_raw_game_json(gid: int | str) -> dict[str, Any]:
    '''Get the raw json from the game page.'''
    # TODO: error logic
    url = GAME_API_TEMPLATE.format(GENDER, gid)
    return _get_raw(url).json()


def get_raw_standings_json(season: int | str) -> dict[str, Any]:
    '''Get the raw json from the standings page.'''
    # TODO: parameter validation
    url = STANDINGS_TEMPLATE.format(GENDER, season)
    # TODO: error logic
    resp = _get_raw(url)

    soup = BeautifulSoup(resp.text, 'html.parser')
    standings_raw = ''
    for x in soup.find_all('script'):
        if str(x).startswith('<script>window'):
            standings_raw = str(x).removeprefix(
                '<script>').removesuffix('</script>')
            break

    if standings_raw == '':
        # TODO: error logic
        pass

    # EXPLANATION
    # - regex split finds assignments for the window object's keys
    # - the second instance of this contains the data we want
    # - remove the residual JS semicolons
    # - load in the cleaned string as json
    # TODO: maybe have a try except block here to handle all the possible
    #       errors in this sqeuence
    standings_json = json.loads(
        re.split(r"window\[.*?\]=", standings_raw)[2].replace(';', ''))
    return standings_json

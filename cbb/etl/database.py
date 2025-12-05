from contextlib import closing
from pathlib import Path
import logging

import sqlite3

logger = logging.getLogger(__name__)

SUBMODULE_DIR = Path(__file__).parent
SQL_DIR = SUBMODULE_DIR / 'sql'

DB_DIR = Path.cwd() / 'db'
DB_FILENAME = 'cbb.db'
DB_FILE = DB_DIR / DB_FILENAME


def init_db(erase: bool = False):
    '''
    (Re-)initializes the database file.
    If `erase` is True and file exists, erases the old DB.
    '''
    if erase and DB_FILE.exists():
        resp = ''
        while resp not in ('y', 'n'):
            usr_in = input(
                f'Are you sure you want to delete {DB_FILENAME}? (Y/[N]) '
            ).strip()

            resp = usr_in.lower()
            if usr_in == '':
                resp = 'n'
            resp = resp[0]

        if resp == 'y':
            logger.debug(f'Deleting old {DB_FILENAME}')
            DB_FILE.unlink(missing_ok=True)
        else:
            logger.debug(f'Keeping old {DB_FILENAME}')

    with (
        closing(sqlite3.connect(DB_FILE)) as conn,
        open(SQL_DIR / 'create_tables.sql', 'r+') as sql_fp
    ):
        cursor = conn.cursor()
        sql_script = sql_fp.read()
        cursor.executescript(sql_script)
        conn.commit()


def main():
    init_db(erase=True)


if __name__ == '__main__':
    main()

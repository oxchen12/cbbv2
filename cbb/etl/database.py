from pathlib import Path

import sqlite3

SUBMODULE_DIR = Path(__file__).parent
SQL_DIR = SUBMODULE_DIR / 'sql'

DB_DIR = Path.cwd() / 'db'


def main():
    with (
        sqlite3.connect(DB_DIR / 'cbb.db') as conn,
        open(SQL_DIR / 'create_tables.sql', 'r+') as sql_fp
    ):
        cursor = conn.cursor()
        sql_script = sql_fp.read()
        cursor.executescript(sql_script)
        conn.commit()


if __name__ == '__main__':
    main()

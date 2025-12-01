import sqlite3


def main():
    with (
        sqlite3.connect('db/cbb.db') as conn,
        open('cbb/etl/sql/create_tables.sql', 'r+') as sql_fp
    ):
        cursor = conn.cursor()
        sql_script = sql_fp.read()
        cursor.executescript(sql_script)
        conn.commit()


if __name__ == '__main__':
    main()

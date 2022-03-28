from airflow.providers.postgres.hooks.postgres import PostgresHook, CursorType


def _get_db_cursor() -> CursorType:
    redshift = PostgresHook('redshift')
    conn = redshift.get_conn()
    cursor = conn.cursor()
    return cursor


def check_districts_table(min_rows: int = 401, max_rows_with_nulls: int = 0) -> None:
    cursor = _get_db_cursor()
    cursor.execute('SELECT count(*) FROM districts')
    num_rows = cursor.fetchone()[0]
    if num_rows < min_rows:
        raise Exception(f'Districts table contains {num_rows} rows, '
                        f'but expected at least {min_rows}.')

    cursor.execute('SELECT count(*) FROM districts'
                   'WHERE district_id IS NULL OR population IS NULL')
    num_rows_with_nulls = cursor.fetchone()[0]
    if num_rows_with_nulls > max_rows_with_nulls:
        raise Exception(f'Districts table contains {num_rows} rows with nulls, '
                        f'but expected at most {min_rows}.')


def check_cases_table(min_rows: int = 8000):
    cursor = _get_db_cursor()
    cursor.execute('SELECT count(*) FROM cases')
    num_rows = cursor.fetchone()[0]
    if num_rows < min_rows:
        raise Exception(f'Cases table contains {num_rows} rows, '
                        f'but expected at least {min_rows}.')


def check_vaccinations_table(min_rows: int = 280_000):
    cursor = _get_db_cursor()
    cursor.execute('SELECT count(*) FROM vaccinations')
    num_rows = cursor.fetchone()[0]
    if num_rows < min_rows:
        raise Exception(f'Vaccinations table contains {num_rows} rows, '
                        f'but expected at least {min_rows}.')

import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset(kinds={"duckdb"})
def users(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE users AS
                        SELECT
                            id,
                            first_name,
                            last_name,
                            sex,
                            date_of_birth::date as date_of_birth,
                            hometown,
                            st_point(longitude::double, latitude::double) as geom
                        FROM 'data/users.csv'
                     """)

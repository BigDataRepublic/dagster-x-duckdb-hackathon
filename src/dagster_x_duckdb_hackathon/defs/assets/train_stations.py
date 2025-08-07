import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset(kinds={"duckdb"})
def train_stations(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE train_stations AS
                        SELECT
                            id,
                            name,
                            country,
                            is_city,
                            is_main_station,
                            is_airport,
                            st_point(longitude::double, latitude::double) as geom
                        FROM 'data/train_stations_europe.csv'
                        WHERE latitude != 'NA' AND longitude != 'NA'
                     """)


@dg.asset(deps=[train_stations], kinds={"duckdb"})
def main_train_stations(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE main_train_stations AS
                        SELECT
                            *
                        FROM train_stations
                        WHERE is_main_station = TRUE
                     """)

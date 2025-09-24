import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset(kinds={"duckdb", "bronze"}, group_name="airports")
def airports(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE airports AS
                        SELECT
                            * exclude (latitude_deg, longitude_deg),
                            st_point(longitude_deg::double, latitude_deg::double) as geom
                        FROM 'data/airports.csv'
                        WHERE continent = 'EU'
                            AND type = 'large_airport'
                     """)


@dg.asset(kinds={"duckdb", "bronze"}, group_name="train_stations")
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
                        WHERE latitude != 'NA'
                            AND longitude != 'NA'
                            AND is_main_station = True
                     """)

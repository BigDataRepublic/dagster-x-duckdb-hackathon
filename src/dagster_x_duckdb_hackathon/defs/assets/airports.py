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
                     """)


@dg.asset(deps=[airports], kinds={"duckdb", "silver"}, group_name="airports")
def eu_airports(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE eu_airports AS
                        SELECT
                            *
                        FROM airports
                        WHERE continent = 'EU' AND type = 'large_airport'
                     """)

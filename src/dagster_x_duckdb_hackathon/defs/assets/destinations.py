import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset(kinds={"duckdb"})
def destinations(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE destinations AS
                        SELECT
                            row_number() OVER () as id,
                            * exclude (latitude, longitude),
                            st_point(longitude::double, latitude::double) as geom
                        FROM 'data/destinations.csv'
                     """)

import dagster as dg
from dagster_duckdb import DuckDBResource

from .destinations import destinations
from .train_stations import main_train_stations


@dg.asset(deps=[destinations, main_train_stations], kinds={"duckdb"})
def destination_train_stations(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE destination_train_stations AS
                        SELECT
                            d.id as destination_id,
                            t.id as train_station_id,
                            ST_Distance_Sphere(d.geom, t.geom) as distance
                        FROM
                            destinations d CROSS JOIN main_train_stations t
                        QUALIFY row_number() OVER (PARTITION BY destination_id ORDER BY distance) = 1
                     """)

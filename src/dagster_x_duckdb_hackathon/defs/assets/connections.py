import dagster as dg
from dagster_duckdb import DuckDBResource

from .airports import eu_airports
from .destinations import destinations
from .train_stations import main_train_stations
from .users import users


@dg.asset(deps=[destinations, main_train_stations], kinds={"duckdb", "silver"}, group_name="connections")
def destination_train_stations(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE destination_train_stations AS
                        SELECT
                            d.id as destination_id,
                            t.id as train_station_id,
                            ST_Distance_Sphere(d.geom, t.geom) as distance,
                            t.geom as geom,
                        FROM
                            destinations d JOIN main_train_stations t ON ST_Distance_Sphere(d.geom, t.geom) < 100000
                        QUALIFY row_number() OVER (PARTITION BY destination_id ORDER BY distance) = 1
                     """)


@dg.asset(deps=[destinations, main_train_stations], kinds={"duckdb", "silver"}, group_name="connections")
def destination_airports(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE destination_airports AS
                        SELECT
                            d.id as destination_id,
                            a.id as airport_id,
                            ST_Distance_Sphere(d.geom, a.geom) as distance,
                            a.geom as geom
                        FROM
                            destinations d JOIN eu_airports a ON ST_Distance_Sphere(d.geom, a.geom) < 100000
                        QUALIFY row_number() OVER (PARTITION BY destination_id ORDER BY distance) = 1
                     """)


@dg.asset(deps=[users, main_train_stations], kinds={"duckdb", "silver"}, group_name="connections")
def user_train_stations(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE user_train_stations AS
                        SELECT
                            u.id as user_id,
                            t.id as train_station_id,
                            ST_Distance_Sphere(u.geom, t.geom) as distance,
                            t.geom as geom,
                        FROM
                            users u CROSS JOIN main_train_stations t
                        QUALIFY row_number() OVER (PARTITION BY user_id ORDER BY distance) = 1
                     """)


@dg.asset(deps=[users, eu_airports], kinds={"duckdb", "silver"}, group_name="connections")
def user_airports(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE user_airports AS
                        SELECT
                            u.id as user_id,
                            a.id as airport_id,
                            ST_Distance_Sphere(u.geom, a.geom) as distance,
                            a.geom as geom
                        FROM
                            users u CROSS JOIN eu_airports a
                        QUALIFY row_number() OVER (PARTITION BY user_id ORDER BY distance) = 1
                     """)


@dg.asset(deps=[user_train_stations, destination_train_stations], kinds={"duckdb", "silver"}, group_name="connections")
def connections_train_stations(context, duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE connections_train_stations AS
                        SELECT DISTINCT
                            o.user_id,
                            d.destination_id,
                            ST_Distance_Sphere(o.geom, d.geom) as connection_distance,
                            'train' as transport_mode,
                            connection_distance / 1000 / 100 as travel_time
                        FROM
                            user_train_stations o CROSS JOIN destination_train_stations d
                     """)

        df = conn.execute("SELECT * FROM connections_train_stations LIMIT 10").df()
        context.add_output_metadata(metadata={"preview": dg.MetadataValue.md(df.to_markdown())})


@dg.asset(deps=[user_airports, destination_airports], kinds={"duckdb", "silver"}, group_name="connections")
def connections_airports(context, duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE connections_airports AS
                        SELECT DISTINCT
                            o.user_id,
                            d.destination_id,
                            ST_Distance_Sphere(o.geom, d.geom) as connection_distance,
                            'flight' as transport_mode,
                            connection_distance / 1000 / 500 as travel_time
                        FROM
                            user_airports o CROSS JOIN destination_airports d
                     """)

        df = conn.execute("SELECT * FROM connections_airports LIMIT 10").df()
        context.add_output_metadata(metadata={"preview": dg.MetadataValue.md(df.to_markdown())})


@dg.asset(deps=[connections_airports, connections_train_stations], kinds={"duckdb", "silver"}, group_name="connections")
def connections(context, duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE connections AS
                        FROM connections_train_stations
                        UNION ALL BY NAME
                        FROM connections_airports
                     """)

        df = conn.execute("SELECT * FROM connections LIMIT 10").df()
        context.add_output_metadata(metadata={"preview": dg.MetadataValue.md(df.to_markdown())})

import dagster as dg
from dagster_duckdb import DuckDBResource


def build_nearest_hub_asset(location_asset_key: str, hub_asset_key: str) -> dg.AssetsDefinition:
    asset_key = f"nearest_{location_asset_key}_{hub_asset_key}"

    @dg.asset(
        name=asset_key,
        deps=[dg.AssetKey(location_asset_key), dg.AssetKey(hub_asset_key)],
        kinds={"duckdb", "silver"},
        group_name="connections",
    )
    def nearest_hub_asset(duckdb: DuckDBResource):
        with duckdb.get_connection() as conn:
            conn.execute(f"""
                        CREATE OR REPLACE TABLE {asset_key} AS
                            SELECT
                                d.id as {location_asset_key}_id,
                                t.id as {hub_asset_key}_id,
                                ST_Distance_Sphere(d.geom, t.geom) as distance,
                                t.geom as geom,
                            FROM
                                destinations d CROSS JOIN {hub_asset_key} t
                            QUALIFY
                                row_number() OVER (PARTITION BY {location_asset_key}_id ORDER BY distance) = 1
                        """)

    return nearest_hub_asset


@dg.asset(
    deps=[dg.AssetKey("users_train_stations"), dg.AssetKey("destinations_train_stations")],
    kinds={"duckdb", "silver"},
    group_name="connections",
)
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
                            user_train_stations o CROSS JOIN destinations_train_stations d
                     """)

        df = conn.execute("SELECT * FROM connections_train_stations LIMIT 10").df()
        context.add_output_metadata(metadata={"preview": dg.MetadataValue.md(df.to_markdown())})


@dg.asset(
    deps=[dg.AssetKey("users_airports"), dg.AssetKey("destinations_airports")],
    kinds={"duckdb", "silver"},
    group_name="connections",
)
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
                            user_airports o CROSS JOIN destinations_airports d
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


@dg.definitions
def defs():
    location_asset_keys = ["users", "destinations"]
    hub_asset_keys = ["train_stations", "airports"]

    nearest_hub_assets = [build_nearest_hub_asset(loc, hub) for loc in location_asset_keys for hub in hub_asset_keys]

    return dg.Definitions(assets=[*nearest_hub_assets, connections_airports, connections_train_stations, connections])

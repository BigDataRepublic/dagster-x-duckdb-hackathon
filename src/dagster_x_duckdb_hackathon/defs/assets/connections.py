import dagster as dg
from dagster_duckdb import DuckDBResource


def build_nearest_hub_asset(location_asset_key: str, hub_asset_key: str) -> dg.AssetsDefinition:
    asset_key = f"{location_asset_key}_{hub_asset_key}"

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
                                {location_asset_key} d CROSS JOIN {hub_asset_key} t
                            QUALIFY
                                row_number() OVER (PARTITION BY {location_asset_key}_id ORDER BY distance) = 1
                        """)

    return nearest_hub_asset


# TODO: add your final definitions for dynamically-created assets here, as `dg.Definitions`.
# @dg.definitions
# def defs():
#     location_asset_keys = ["users", "destinations"]
#     hub_asset_keys = [...]
#
#     nearest_hub_assets = [...]
#
#     return dg.Definitions(assets=[...])

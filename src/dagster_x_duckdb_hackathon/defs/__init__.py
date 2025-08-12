import dagster as dg

from .resources import DuckDBSpatialResource
from .sensors import destination_sensor

defs = dg.Definitions(
    sensors=[destination_sensor],
    resources={
        "duckdb": DuckDBSpatialResource(database="db.duckdb"),
    },
)

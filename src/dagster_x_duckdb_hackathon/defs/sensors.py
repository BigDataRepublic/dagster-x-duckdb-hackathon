import dagster as dg

from .partitions import destination_partitions
from .resources import DuckDBSpatialResource


@dg.sensor(
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
)
def destination_sensor(context: dg.SensorEvaluationContext, duckdb: DuckDBSpatialResource):
    with duckdb.get_connection() as conn:
        destinations = conn.execute("SELECT DISTINCT id FROM destinations").fetchall()
        if not destinations:
            return dg.SensorResult()
        keys_in_db = {str(r[0]) for r in destinations}

    keys_in_def = set(context.instance.get_dynamic_partitions("destination_partitions"))

    add_keys = sorted(keys_in_db - keys_in_def)

    return dg.SensorResult(
        # run_requests=[dg.RunRequest(partition_key=destination) for destination in add_keys],
        dynamic_partitions_requests=[
            destination_partitions.build_add_request(add_keys),
        ],
    )

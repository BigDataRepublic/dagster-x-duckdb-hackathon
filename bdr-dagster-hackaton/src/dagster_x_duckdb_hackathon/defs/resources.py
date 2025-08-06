import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "duckdb": DuckDBResource(
                database="db.duckdb",  # required
            )
        }
    )

from contextlib import contextmanager

import dagster as dg
from dagster_duckdb import DuckDBResource


class DuckDBSpatialResource(DuckDBResource):
    @contextmanager
    def get_connection(self):
        with super().get_connection() as conn:
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")
            yield conn


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "duckdb": DuckDBSpatialResource(
                database="db.duckdb",
            )
        }
    )

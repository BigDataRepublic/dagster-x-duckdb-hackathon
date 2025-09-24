from contextlib import contextmanager

from dagster_duckdb import DuckDBResource


class DuckDBSpatialResource(DuckDBResource):
    @contextmanager
    def get_connection(self):
        with super().get_connection() as conn:
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")
            yield conn

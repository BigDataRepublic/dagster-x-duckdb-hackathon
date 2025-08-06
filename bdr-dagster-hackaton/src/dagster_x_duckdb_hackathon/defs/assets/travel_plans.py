import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset()
def travel_plans(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS travel_plans AS SELECT * FROM 'data/destinations.csv'")

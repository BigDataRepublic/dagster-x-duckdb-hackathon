import dagster as dg
from dagster_duckdb import DuckDBResource

from .destinations import destinations
from .users import users


@dg.asset(deps=[users, destinations], kinds={"duckdb"})
def user_destinations(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE user_destinations AS
                        SELECT u.*, d.*
                        FROM users u
                        CROSS JOIN destinations d
                     """)

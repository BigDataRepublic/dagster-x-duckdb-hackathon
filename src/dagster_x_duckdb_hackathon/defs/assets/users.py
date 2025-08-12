from hashlib import sha256
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.observable_source_asset(group_name="users")
def users_file():
    content = Path("data/users.csv").read_text()
    content_hash = sha256(content.encode()).hexdigest()
    return dg.DataVersion(content_hash)


@dg.asset(kinds={"duckdb", "bronze"}, deps=[users_file], group_name="users")
def users(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE users AS
                        SELECT
                            id,
                            first_name,
                            last_name,
                            sex,
                            date_of_birth::date as date_of_birth,
                            hometown,
                            st_point(longitude::double, latitude::double) as geom,
                            preferred_temperature,
                            preferred_travel_time,
                            preferred_transport_mode
                        FROM 'data/users.csv'
                     """)

        df = conn.execute("SELECT * FROM users LIMIT 10").df()
        count = conn.execute("SELECT count(*) FROM users").fetchone() or (0,)
        count = count[0]
        return dg.MaterializeResult(metadata={"preview": dg.MetadataValue.md(df.to_markdown()), "rows": count})

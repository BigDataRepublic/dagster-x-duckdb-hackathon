import dagster as dg
from dagster_duckdb import DuckDBResource

from .connections import connections
from .destinations import destinations
from .users import users
from .weather import weather_forecast


@dg.asset(
    kinds={"duckdb", "silver"}, deps=[users, destinations, connections, weather_forecast], group_name="recommendations"
)
def recommendations(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE recommendations AS
                        SELECT
                            u.id as user_id,
                            d.id as destination_id,
                            w.month,
                            c.transport_mode,
                            c.travel_time,
                            w.temperature_max,
                            sqrt((u.preferred_travel_time - c.travel_time)**2) AS travel_time_penalty,
                            sqrt((u.preferred_temperature - w.temperature_max)**2) AS temperature_penalty,
                            travel_time_penalty + temperature_penalty AS total_penalty,
                            row_number() over (PARTITION BY u.id ORDER BY total_penalty) as rank,
                        FROM connections c
                            JOIN users u
                                ON u.id = c.user_id
                                AND u.preferred_transport_mode = c.transport_mode
                            JOIN destinations d
                                ON d.id = c.destination_id
                            JOIN weather_forecast w
                                ON w.destination_id = c.destination_id
                        QUALIFY
                            rank <= 8;
                     """)

        df = conn.execute("SELECT * FROM recommendations LIMIT 10").df()
        count = conn.execute("SELECT count(*) FROM recommendations").fetchone() or (0,)
        count = count[0]

        return dg.MaterializeResult(metadata={"preview": dg.MetadataValue.md(df.to_markdown()), "rows": count})


@dg.asset(kinds={"duckdb", "gold"}, deps=[recommendations], group_name="recommendations")
def recommendations_dashboard(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE recommendations_dashboard AS
                        SELECT
                            u.id,
                            u.first_name,
                            u.last_name,
                            d.id,

                        FROM recommendations r
                            JOIN users u
                                ON r.user_id = u.id
                            JOIN destinations d
                                ON r.destination_id = d.id
                     """)

        df = conn.execute("SELECT * FROM recommendations_dashboard LIMIT 10").df()
        count = conn.execute("SELECT count(*) FROM recommendations_dashboard").fetchone() or (0,)
        count = count[0]

        return dg.MaterializeResult(metadata={"preview": dg.MetadataValue.md(df.to_markdown()), "rows": count})

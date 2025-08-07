import dagster as dg
from dagster_duckdb import DuckDBResource

from .train_stations import main_train_stations
from .users import users


@dg.asset(deps=[users, main_train_stations], kinds={"duckdb"})
def user_train_stations(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE user_train_stations AS
                        SELECT
                            u.id as user_id,
                            t.id as train_station_id,
                            ST_Distance_Sphere(u.geom, t.geom) as distance
                        FROM
                            users u CROSS JOIN main_train_stations t
                        QUALIFY row_number() OVER (PARTITION BY user_id ORDER BY distance) = 1
                     """)

        data = conn.execute("FROM user_train_stations").df()
        path = "data/user_train_stations.csv"
        data.to_csv(path)

        return dg.MaterializeResult(
            metadata={
                "file_path": path,
                "row_count": len(data),
            }
        )

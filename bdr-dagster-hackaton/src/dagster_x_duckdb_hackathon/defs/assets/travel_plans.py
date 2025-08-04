import dagster as dg
import pandas as pd
from dagster_duckdb import DuckDBResource


@dg.asset()
def travel_plans(duckdb: DuckDBResource) -> dg.MaterializeResult:
    iris_df = pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )

    with duckdb.get_connection() as conn:
        conn.execute("CREATE TABLE iris_dataset AS SELECT * FROM iris_df")

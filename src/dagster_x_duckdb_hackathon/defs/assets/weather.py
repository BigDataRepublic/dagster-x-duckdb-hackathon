import datetime
from pathlib import Path

import dagster as dg
import openmeteo_requests
import polars as pl
import requests_cache
from dagster_duckdb import DuckDBResource
from dateutil.relativedelta import relativedelta
from retry_requests import retry

from ..partitions import weather_partitions
from .destinations import destinations


def fetch_weather(
    lat: float,
    lon: float,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pl.DataFrame:
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    variables = [
        "temperature_2m_mean",
        "temperature_2m_max",
    ]

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": variables,
        "timezone": "auto",
    }

    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    weather = responses[0]

    daily = weather.Daily()

    data = {}

    for i, variable in enumerate(variables):
        data[variable] = daily.Variables(i).ValuesAsNumpy()

    df = pl.DataFrame(data)
    df = df.with_columns(
        pl.datetime_range(
            start=datetime.date.fromtimestamp(daily.Time()),
            end=datetime.date.fromtimestamp(daily.TimeEnd()),
            closed="right",
            interval="1d",
        ).alias("date")
    )

    return df


@dg.asset(
    partitions_def=weather_partitions,
    deps=[destinations],
    kinds={"bronze", "python"},
    group_name="weather",
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def historic_weather_by_destination_yearly(
    context,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    mpk: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    start_date = datetime.date.fromisoformat(mpk["year"])
    end_date = start_date + relativedelta(years=1, days=-1)

    destination_id = mpk["destination"]

    out_path = Path(f"data/weather/destination_id={destination_id}/year={start_date.year}")

    if not out_path.exists():
        with duckdb.get_connection() as conn:
            row = conn.execute(
                """
                    SELECT st_y(geom) as lat, st_x(geom) as lon
                    FROM destinations
                    WHERE id = $id
                """,
                parameters={"id": destination_id},
            ).fetchone()

            if row is not None:
                lat, lon = row

            else:
                raise ValueError(destination_id)

            # Write weather to disk
            weather = fetch_weather(lat, lon, start_date, end_date)

            out_path.mkdir(parents=True, exist_ok=True)
            weather.write_parquet(out_path / "weather.parquet")

    else:
        weather = pl.read_parquet(out_path)

    # return materialize result
    return dg.MaterializeResult(
        metadata={
            "first_day": str(weather["date"].min()),
            "last_day": str(weather["date"].max()),
            "days": (end_date - start_date).days + 1,
        }
    )


@dg.asset(deps=[historic_weather_by_destination_yearly], kinds={"duckdb", "silver"}, group_name="weather")
def weather(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE weather AS
                        SELECT
                            *
                        FROM read_parquet('data/weather/*/*/*.parquet', hive_partitioning = true)
                     """)
        df = conn.execute("SELECT * FROM weather LIMIT 10").df()
        return dg.MaterializeResult(metadata={"preview": dg.MetadataValue.md(df.to_markdown())})


@dg.asset(deps=[weather], kinds={"duckdb", "silver"}, group_name="weather")
def weather_forecast(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
                     CREATE OR REPLACE TABLE weather_forecast AS
                        SELECT
                            destination_id,
                            monthname(date) as month,
                            avg(temperature_2m_mean) as temperature_mean,
                            avg(temperature_2m_max) as temperature_max,
                        FROM weather
                        GROUP BY
                            destination_id, month
                     """)
        df = conn.execute("SELECT * FROM weather_forecast LIMIT 10").df()
        return dg.MaterializeResult(metadata={"preview": dg.MetadataValue.md(df.to_markdown())})

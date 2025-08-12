import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
    Use this notebook to collect historic weather data using the https://archive-api.open-meteo.com/v1/archive api.

    Latitude and longitude are loaded from a csv files.

    By default the notebook collects historic data starting from 2019-01-01 until yesterday's date. You can change this behaviour by changing the `START_DATE` and `END_DATE` variables.
    """
    )
    return


@app.cell
def _():
    import openmeteo_requests
    import pandas as pd
    import requests_cache
    from openmeteo_requests.Client import WeatherApiResponse
    from retry_requests import retry
    import datetime
    import time

    return (
        WeatherApiResponse,
        datetime,
        openmeteo_requests,
        pd,
        requests_cache,
        retry,
        time,
    )


@app.cell
def _(pd):
    # destination csv path
    destination_csv_path = "data/destinations.csv"

    # load data
    data = pd.read_csv(destination_csv_path)

    # extract Destination as CityName and Latitudine and Longitude
    subset = data[["Destination", "Latitude", "Longitude"]]
    return (subset,)


@app.cell
def _(subset):
    rows = [row for row in subset.iterrows()]
    return (rows,)


@app.cell
def _(
    WeatherApiResponse,
    datetime,
    openmeteo_requests,
    pd,
    requests_cache,
    retry,
):
    START_DATE = "2019-01-01"
    #set the end date to yesterday and format it as YYYY-MM-DD
    END_DATE = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    TIMEZONE = "Europe/Berlin"
    def get_weather(lat: float, lon: float) -> WeatherApiResponse:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "daily": ["precipitation_sum", "snowfall_sum", "temperature_2m_mean"],
            "hourly": "temperature_2m",
            "timezone": TIMEZONE,
        }
        responses = openmeteo.weather_api(url, params=params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]
        return response

    def weather_response_to_hourly_dataframe(city: str, weather: WeatherApiResponse) -> pd.DataFrame:
        print(f"Coordinates: {weather.Latitude()}°N {weather.Longitude()}°E")
        print(f"Elevation: {weather.Elevation()} m asl")
        print(f"Timezone difference to GMT+0: {weather.UtcOffsetSeconds()}s")

        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = weather.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            ),
            "city": city,
            "latitude": weather.Latitude(),
            "longitude": weather.Longitude(),
            "elevation": weather.Elevation(),
            "utc_offset_seconds": weather.UtcOffsetSeconds(),
            "timezone": weather.Timezone(),
        }

        hourly_data["temperature_2m"] = hourly_temperature_2m

        hourly_dataframe = pd.DataFrame(data=hourly_data)
        print("\nHourly data\n", hourly_dataframe)
        return hourly_dataframe

    return get_weather, weather_response_to_hourly_dataframe


@app.cell
def _(
    datetime,
    get_weather,
    pd,
    rows,
    time,
    weather_response_to_hourly_dataframe,
):
    # execute code in parallel


    now = datetime.datetime.now().isoformat().replace(":", "-").replace(".", "-")
    results = []
    is_first = False
    # divide the rows in chunks of 10
    # and execute the get_weather function in parallel
    failed = []
    for row in rows:
        city = row[1]["Destination"]
        lat = row[1]["Latitude"]
        lon = row[1]["Longitude"]
        try:
            print(f"Processing row: {city}")
            result = get_weather(lat, lon)
            results.append({city: result})
        except Exception as e:
            failed.append(row)
            time.sleep(60)
            result = get_weather(lat, lon)
            results.append({city: result})
            print(f"Error processing row {row}: {e}")
    for r in results:
        for city, weather_response in r.items():
            if not is_first:
                df = weather_response_to_hourly_dataframe(city, weather_response)
                is_first = True
            else:
                df = pd.concat([df, weather_response_to_hourly_dataframe(city, weather_response)], ignore_index=True)

    # save the dataframe to a csv file
    df.to_parquet(f"bdr-dagster-hackaton/data/{now}_historical_weather_data.parquet", index=False)
    return (results,)


@app.cell
def _(results):
    results[0]
    return


if __name__ == "__main__":
    app.run()

# bdr-dagster-hackaton

## Installation
1. `git clone git@github.com:consumer-tech/intelligent-data-platform.git`
2. `cd bdr-dagster-hackaton`
3. `uv sync --dev`
4. `export DAGSTER_HOME=/tmp/bdr_dagster_hackaton`
5. `uv run dg dev`

## Tasks
1. Create an asset based on `destination.csv`
2. Based on the user location, compute the closest airports and train stations
   1. Hint: load `trains.csv` and `airport.csv` as asset and materialize the result as an asset.
3. Query the weather endpoint\fetch weather conditions and create a weather asset
   1. Clean the weather asset
4. Load the `historical_weather_data` and combine them with the weather asset to create a forecast based on the user details.
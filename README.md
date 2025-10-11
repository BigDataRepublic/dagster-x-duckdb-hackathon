# BDR Dagster Hackathon

Welcome to the repo for BDR's Dagster Hackathon!

We will be taking on the role of a startup that wants to come up with dream
holiday ideas for their customers. We will do this by building a new 
data product that creates travel recommendations, based on multiple data
sources, and is orchestrated by Dagster.

Read on on how to use this repo as a guided tutorial and how to setup your
local development environment.

The **assignments** are also listed further in this document.

## How to use this repo

We will teach different ways to use Dagster through several tasks. The tasks
are available on the `main` branch and are listed in this README. We also 
offer our own solution on the `hackathon-final` branch.

While you are expected to code up your own solution to the tasks below,
you are encouraged to consult with online documentation, for example:

* Dagster docs:
  * [Defining assets](https://docs.dagster.io/guides/build/assets/defining-assets)
  * [Build your first ETL pipeline](https://docs.dagster.io/examples/full-pipelines/etl-pipeline)
* DuckDB:
  * [Spatial functions](https://duckdb.org/docs/stable/core_extensions/spatial/functions.html)

## Setup

### System requirements

* Python :)
* [uv](https://docs.astral.sh/uv/guides/install-python/) Python package manager. 
* Install DuckDB, for example with `brew install duckdb`
* Generate user data with: `uv run data/usergen.py`

### Project installation

1. Sync you local development environment with: `uv sync --dev`
2. Set an environment variable with `export DAGSTER_HOME=/tmp/bdr_dagster_hackaton`
3. Start Dagster with `uv run dg dev` and open the [web UI](http://127.0.0.1:3000)

**NOTE**: ignore the sensor errors in the terminal for now.

## Asset overview
At the heart of this project are the Dagster assets that bring our dream-holiday recommendation engine to life. Think of these assets as the different departments of our little startup: each one has a role to play in turning raw data into personalized travel advice for our customers.

![asset_overview.png](asset_overview.png)

### Core Data Assets

This is where we gather the foundational information – our raw ingredients.

- **Users**: Who are our customers, where do they live, and what do they prefer (temperature, travel time, transport mode)?

- **Destinations**: A collection of possible holiday spots across Europe, enriched with geospatial points.

- **Airports**: Large airports across Europe, filtered from open data sources.

- **Train Stations**: Major European train stations, ensuring connectivity for rail-loving travelers.

- **Historic Weather**: Yearly weather data for destinations, fetched from the [Open-Meteo API](https://open-meteo.com/en/docs)

### Connections & Enrichment

Here we process and combine the data, much like planning out the routes, tickets and weather info that make trips feasible.

**Nearest Hubs**: For both users and destinations, we find the closest airports and train stations.

**Connections (Airports & Train Stations)**: Calculate travel distances and approximate travel times between users and destinations by different modes of transport.

**Unified Connections**: Merge air and rail connections into a single dataset for comparison.

**Weather Forecast**: From raw weather data, we aggregate average monthly conditions so customers know what to expect.

### Recommendations

Finally, the magic happens. This is where we bring everything together into a recommendation engine.

**Recommendations**: For each user, we combine travel preferences, weather forecasts and travel connections to generate ranked holiday suggestions. We calculate a “penalty score” for each option (based on mismatches in travel time and preferred temperature) and return the top destinations.

### Marimo

Once we have our recommendations, we can have a look at them in marimo by running 

```
uv run marimo run dashboard.py
```

---

## Assignments

The data assets as described, however, are not fully implemented yet. That's where you come in. By completing the following assignments, we will bring together the recommondation pipeline one step at a time. We will start off with a quick introduction to Dagster's most important features. 

Please note, if you ever get stuck, please reach out or check the final solution branch `hackathon-final` for inspiration.

### :egg: Task 1: Getting started

_Start a local Dagster instance and materialize an asset._

To start you off on your Dagster journey, we have provided you with some 
initial code. It includes a [users](./src/dagster_x_duckdb_hackathon/defs/assets/users.py) 
asset, based off of the [users.csv](./data/users.csv), and a sensor
functionality which monitors for changes in the source file (optional feature
that you do not need to implement further on).

Your task is to setup your local environment, start a local Dagster instance
(`uv run dg dev`), and materialize your first asset.

Once you have a local Dagster instance running, navigate to the "Assets" section
in the top menu bar, then click on "View lineage" and select the `users` asset,
and finally click to "Materialize" it. This should start a new run, whose 
execution you can also follow.

Once the asset has been materialized, you should be able to preview some data in
the UI, under the "Latest materialization" section.

(Materializing the `users` asset in the UI also resolves the sensor error in the terminal!)

---

### :seedling: Task 2: Creating simple assets

_Create assets from CSVs as inputs._

#### Part I – You very own first asset:

Now you are ready to code your own first asset from scratch!

Given the CSV file [destinations.csv](./data/destinations.csv), and the file
[destinations.py](./src/dagster_x_duckdb_hackathon/defs/assets/destinations.py)
(where you can start coding your solution), create a new asset called 
`destinations` and read in all the columns from the CSV, giving each row a 
unique ID.

Test out your solution by starting the local Dagster instance and 
materializing your new asset in the UI, just like you did in Task 1.

**TIP**: you can use the [st_point](https://duckdb.org/docs/stable/core_extensions/spatial/functions.html#st_point)
function to read in longitude and latitude as a GEOMETRY type (from the 
spatial extension).

#### Part II - Two more!:

After creating the `desinations` asset, you can continue creating two more
simple assets:

* `airports` – based off the [airports.csv](./data/airports.csv)
* `train_stations` – based off the [train_stations_europe.csv](./data/train_stations_europe.csv)

You can create the new assets in their own files (or a single new file) in the 
`assets` directory alongside the `destinations` asset you've coded previously.
Remember to materialize them in the UI to test out your solution.

---

### :rabbit2: Task 3: Computed asset

_Create assets that link the users with potential destinations, calculating the
shortest distance and travel time._

This will help us select optimal routes for our holiday ideas later on.

Alongside our `destinations` asset, you are now given the following new assets:
* [users](./src/dagster_x_duckdb_hackathon/defs/assets/users.py)
* [airports](./src/dagster_x_duckdb_hackathon/defs/assets/hubs.py)
* [train_stations](./src/dagster_x_duckdb_hackathon/defs/assets/hubs.py)

(Materialize the `users` asset in the UI to resolve the sensor error in the terminal)

Take note of the helper function [build_nearest_hub_asset](./src/dagster_x_duckdb_hackathon/defs/assets/connections.py)
that can dynamically generate an asset that represents closest connections by 
geometric distance.

#### Part I - Intermediate assets:

Create the following (intermediate) computed assets:
* `destinations_airports`
* `destinations_train_stations`
* `users_airports`
* `users_train_stations`

Linking all users and destinations with all transport hubs, using the 
`build_nearest_hub_asset` helper function.

#### Part II – Connecting it all together:

Create the following computed assets:
* `connections_train_stations` – using `users_train_stations` and `destinations_train_stations`
* `connections_airports` – using `users_airports` and `destinations_airports`
* `connections` (union of the two above)

With the following schema:
* `users_id` – an ID from the users table.
* `destinations_id` – and ID from the destinations table.
* `connection_distance` – (linear) distance between the two.
* `transport_mode` – 'train' or 'flight'.
* `travel_time` – estimated based on connection_distance.

---

### :sun_with_face: Task 4: External asset

_Query an (external) API for weather data._

In order to select the right kind of holiday destination we would like to use
weather data to match it with our users' preferences.

Create an asset that reads historical weather data from Open Meteo. 

You can try to create an asset that reads from the API directly by reading the [Dagster docs](https://docs.dagster.io/guides/build/external-resources/connecting-to-apis).

**NOTE**: Alternatively, use the [get_historical_data](./data/get_historical_data.py) script to dump the historical
data to a file, and proceed like before by reading CSV files.

---

### :rocket: Task 5: Open ended

_Generate travel recommendations based on input data._

Now that we have collected and processed data from different sources,
we are ready to generate travel recommendations for our users.

The solution of this assignment is meant to be open-ended. One solution would be to use
`travel_time` and `preferred_temperature` to rank destinations for all users.

You may also think about expanding on the given input data by defining new assets or even plugging in a 
`scikit-learn` model [into Dagster](https://docs.dagster.io/guides/build/ml-pipelines/ml-pipeline). 

**NOTE**: As mentioned in the data overview section, we have provided a ranking solution in the `hackathon-final` branch as an example. However, 
you are encouraged to experiment and find things out on your own.

**NOTE**: Don't forget about the marimo dashboard! 

---

### :books: Task 6 (bonus round): Data engineering improvements 

_Explore Dagster's features regarding data engineering_

Dagster offers a lot of features related to data engineering. In this bonus assignment, you're encouraged to implement (in order of increasing complexity): 

- Add relevant metadata to the implemented assets 
- Making an asset parameterizable through `@asset(config_schema=...)`
- Different types of partitioning, considering which makes the most sense for our assets
- Explore `@dg.observable_source_asset` and `@dg.sensor` to check data freshness

**NOTE**: We also included implementations of these features in the `hackathon-final` branch.

## Debugging

### DuckDB

Start DuckDB with this project's database with: `duckdb db.duckdb`

**NOTE**: Opening up the DuckDB client in the terminal will block the database
connection for Dagster itself! So make sure you exit it before materializing 
assets.

Type `.help` for a list of commands, but otherwise you can use it in a similar
way to other SQL database. Some examples:

List tables: `.tables`

Select users: `SELECT * FROM users LIMIT 10;`

Read 
```
SELECT
    row_number() OVER () as id,
    * exclude (latitude, longitude),
    st_point(longitude::double, latitude::double) as geom
FROM 'data/destinations.csv';
```

### Marimo notebooks

Some tips on using the [Marimo notebooks](https://docs.marimo.io/#a-reactive-programming-environment):

Enter edit mode for the dashboard with: `uv run marimo edit dashboard.py`

Marimo tutorial: `uv run marimo tutorial intro`

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

We will teach different ways to use Dagster through several tasks. Each task
can be accessed by checking out a different branch:

* `main` – contains only the instructions on how to setup your local 
 environment and to use this repo.
* `hackathon-task1` – ready for the implementation of Task 1.
* `hackathon-task2` – ready for the implementation of Task 2.
* `hackathon-task3` – ready for the implementation of Task 3.
* `hackathon-final` – our full solution, with a suggestion left open-ended for
 expansion.

**NOTE**: We advise you to create a new branch when working on your tasks,
as we will be providing a working solution after each step. For example,
name your branch something like `hackathon-task2-inge` and commit your changes.

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

## Assignments

### :seedling: Task 1: Create your first asset

_Create a new asset from a CSV about potential holiday destinations._

To start you off on your Dagster journey, you are given a CSV file. Create a
new asset called `destinations` and read in all the columns from the CSV,
giving each row a unique ID.

Test out your solution by starting the local Dagster instance and 
materializing your new asset.

**TIP**: you can use the [st_point](https://duckdb.org/docs/stable/core_extensions/spatial/functions.html#st_point)
function to read in longitude and latitude as a GEOMETRY type (from the 
spatial extension).

---

### :rabbit2: Task 2: Computed asset

_Create assets that link the users with potential destinations, calculating the
shortest distance and travel time._

This will help us select optimal routes for our holiday ideas later on.

Alongside our `destinations` asset, you are now given the following new assets:
* [users](./src/dagster_x_duckdb_hackathon/defs/assets/users.py)
* [airports](./src/dagster_x_duckdb_hackathon/defs/assets/hubs.py)
* [train_stations](./src/dagster_x_duckdb_hackathon/defs/assets/hubs.py)

(Materialize the `users` asset in the UI to resolve the sensor error in the terminal)

As well as a helper function [build_nearest_hub_asset](./src/dagster_x_duckdb_hackathon/defs/assets/connections.py)
that can dynamically generate an asset that represents closest connections by 
geometric distance.

#### Part 1:

Create the following (intermediate) computed assets:
* `destinations_airports`
* `destinations_train_stations`
* `users_airports`
* `users_train_stations`

Linking all users and destinations with all transport hubs, using the 
`build_nearest_hub_asset` helper function.

#### Part 2:

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

### :sun_with_face: Task 3: External asset

_Query an (external) API for weather data._

In order to select the right kind of holiday destination we would like to use
weather data to match it with our users' preferences.

Create an asset that reads historical weather data from Open Meteo. 

You can try to create an asset that reads from the API directly by reading the [Dagster docs](https://docs.dagster.io/guides/build/external-resources/connecting-to-apis).

**NOTE**: Alternatively, use the [get_historical_data](./data/get_historical_data.py) script to dump the historical
data to a file, and proceed like before by reading CSV files.

---

### :rocket: Task 4: Open ended

Generate travel recommendations.

There is a solution available, but you are free to experiment and find things out on your own.

## Debugging

### DuckDB

Start DuckDB with this project's database with: `duckdb db.duckdb`

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

Enter edit mode for the dashboard with: `uv run marimo edit dashboard.py`

Marimo tutorial: `uv run marimo tutorial intro`

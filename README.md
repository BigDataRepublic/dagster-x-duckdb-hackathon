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

### Project installation

1. Sync you local development environment with: `uv sync --dev`
2. Set an environment variable with `export DAGSTER_HOME=/tmp/bdr_dagster_hackaton`
3. Start Dagster with `uv run dg dev` and open the [web UI](http://127.0.0.1:3000)

## Assignments

### Task 1: Simple asset

Create a new asset from a CSV about potential holiday destinations.

To start you off on your Dagster journey, you are given a CSV file.

#### BONUS

Make it a sensor

### Task 2: Computed asset

Create an asset linking users with potential destinations, with the calculated
shortest distance and transport mode, with the travel time. 

HINT: you can generate assets programmatically using functions.

### Task 3: External asset

Query an (external) API for weather data.

NOTE: if we get rate limited, ask for static files instead :)

#### BONUS 

Use partitioning

### Task 4: Open ended

Generate travel recommendations.

There is a solution available, but you are free to experiment and find things out on your own.

## Debugging

### Getting static data

TODO
`usergen.py`

### Sensors, partitions, resources and definitions

...

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

import marimo

__generated_with = "0.14.16"
app = marimo.App(layout_file="layouts/dashboard.grid.json")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import duckdb

    DATABASE_URL = "db.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=True)
    engine.execute("load spatial;");
    return (engine,)


@app.cell
def _(engine, mo):
    users = mo.sql(
        f"""
        from users;
        """,
        output=False,
        engine=engine
    )
    return (users,)


@app.cell
def _(engine, mo, users):
    user_locations = mo.sql(
        f"""
        select id, first_name || ' ' || last_name as name, st_y(geom) as lat, st_x(geom) as lon from users;
        """,
        output=False,
        engine=engine
    )
    return (user_locations,)


@app.cell
def _(destinations, go, mo, user_locations):
    user_fig = go.Figure([
        go.Scattermap(name="users", lat=user_locations["lat"], lon=user_locations["lon"], hovertext=user_locations["name"], opacity=0.5, mode="markers"),
        go.Scattermap(name="destinations", lat=destinations["lat"], hovertext=destinations["Destination"], lon=destinations["lon"], opacity=1, mode="markers"),
    ])

    user_fig.update_layout(
        showlegend=True, margin=dict(l=0, r=0, t=0, b=0), mapbox=dict(center=dict(lon=52, lat=2), zoom=10)
    )
    user_map = mo.ui.plotly(user_fig)
    user_map
    return


@app.cell
def _(engine, mo):
    destinations = mo.sql(
        f"""
         select Destination, st_y(geom) as lat, st_x(geom) as lon from destinations;
        """,
        output=False,
        engine=engine
    )
    return (destinations,)


@app.cell
def _(mo, users):
    user_names = {f"{u[1]} {u[2]}":str(u[0]) for u in users.rows()}
    selected_user = mo.ui.dropdown(user_names, searchable=True, allow_select_none=False, value=list(user_names.keys())[0])
    selected_user
    return (selected_user,)


@app.cell
def _(destinations, engine, mo, selected_user, users):
    recommendations = mo.sql(
        f"""
        select
            u.first_name,
            u.last_name,
            u.hometown,
            d.Destination,
            d.Country,
            d.Description,
            d."Best Time to Visit",
            st_y(u.geom) as lat_start,
            st_x(u.geom) as lon_start,
            st_y(d.geom) as lat_dest,
            st_x(d.geom) as lon_dest,
            r.rank,
            r.month,
            r.transport_mode,
            u.preferred_transport_mode,
            r.travel_time,
            u.preferred_travel_time,
            r.temperature_max,
            r.preferred_temperature
        from recommendations r
            join destinations d on r.destination_id = d.id
            join users u on r.user_id = u.id
        where user_id = {selected_user.value}
        """,
        engine=engine
    )
    return (recommendations,)


@app.cell
def _():
    import plotly.graph_objects as go
    return (go,)


@app.cell
def _(mo, recommendations):
    u = recommendations.rows(named=True)[0]
    mo.md(f"""
    {u["first_name"]} {u["last_name"]} from {u["hometown"]} likes to travel by **{u["preferred_transport_mode"]}** for around **{u["preferred_travel_time"]} hours** to a destination with a temperature of **{u["preferred_temperature"]}C**.
    """)
    return


@app.cell
def _(mo, recommendations):
    mo.ui.table(recommendations[["rank", "Destination", "Country", "month", "Description"]].sort("rank"))
    return


@app.cell
def _(go, mo, recommendations):
    fig = go.Figure()

    for rec in recommendations.rows(named=True):
        fig.add_trace(
            go.Scattermap(
                mode = "markers+lines",
                lon = [rec["lon_start"], rec["lon_dest"]],
                lat = [rec["lat_start"], rec["lat_dest"]],

                marker = {'size': 10},
                text = f"""
                <b>{rec["rank"]} - {rec["Destination"]} in {rec["month"]}</b><br>
                {rec["travel_time"]:.1f}h by {rec["transport_mode"]}<br>
                {rec["temperature_max"]:.1f}C max temperature<br>
                {rec["Description"]}""",
            )
        )

    fig.update_layout(
        showlegend=False,
        margin=dict(l=0, r=0, t=0, b=0),
        mapbox=dict(
            center=dict(lon=52, lat=2),
            zoom=10                           
        )
    )
    map = mo.ui.plotly(fig)
    return (map,)


@app.cell
def _(map):
    map
    return


@app.cell
def _(map):
    map.value
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

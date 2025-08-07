import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import httpx
    import urllib
    from pathlib import Path
    import json
    return Path, httpx, json, urllib


@app.cell
def _():
    example_url = "https://geoservices.zuid-holland.nl/arcgis/rest/services/Anders/Europese_Projectie_EPSG3035/MapServer/88/query?where=&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&uniqueIds=&returnUniqueIdsOnly=false&featureEncoding=esriDefault&f=geojson"
    return (example_url,)


@app.cell
def _(example_url, urllib):
    parsed_url = urllib.parse.urlparse(example_url)
    parsed_query = urllib.parse.parse_qs(parsed_url.query)
    return parsed_query, parsed_url


@app.cell
def _(Path):
    offset = 0
    page_size = 10
    data_path = Path("data/trainlines")
    data_path.mkdir(exist_ok=True)
    return data_path, page_size


@app.cell
def _(data_path, httpx, page_size, parsed_query, parsed_url):
    def download(offset: int):
        while True:
            url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
            query = parsed_query
            query.update({"resultOffset": offset, "resultRecordCount": page_size})
            res = httpx.get(url, params=query, timeout=30)

            try:
                data = res.json()

            except:
                print(f"Offset {offset} failed: {res.status_code}")
                offset += page_size
                continue

            (data_path / f"trainlines_{offset}.geojson").write_bytes(res.content)

            if len(data["features"]) < page_size:
                print("Last page reached. Done!")
                break

            offset += page_size
            print(f"Fetched {offset} trainlines")
    return (download,)


@app.cell
def _(download):
    download(700)
    return


@app.cell
def _(Path, data_path, json):
    combined_geojson = None
    for file in data_path.glob("*.geojson"):
        json_file = json.loads(file.read_text())

        if combined_geojson is None:
            combined_geojson = json_file
            continue

        combined_geojson["features"].extend(json_file["features"])

    Path("data/low_speed_trains.geojson").write_text(json.dumps(combined_geojson))
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        load spatial;
        """
    )
    return


@app.cell
def _(mo):
    unnested = mo.sql(
        f"""
        select name, unnest(st_dump(geom)).geom as geom_unnested, st_length_spheroid(geom_unnested) as length from st_read('data/high_speed_trains.geojson');
        """,
        output=False
    )
    return (unnested,)


@app.cell
def _(unnested):
    unnested
    return


@app.cell
def _(mo, unnested):
    _df = mo.sql(
        f"""
        copy (select name, geom_unnested as geom, length from unnested where length > 2000) to 'data/ls_trains.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON', LAYER_NAME 'low_speed_trains');
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()

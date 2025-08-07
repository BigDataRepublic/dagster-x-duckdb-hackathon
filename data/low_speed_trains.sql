load spatial;

copy (
    with unnested as (
        select name, unnest(st_dump(geom)).geom as geom_unnested, st_length_spheroid(geom_unnested) as length from st_read('data/high_speed_trains.geojson')
    ), filtered as (
        select name, geom_unnested as geom, length from unnested where length > 1000
    )
    from filtered
) to 'data/ls_trains.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');


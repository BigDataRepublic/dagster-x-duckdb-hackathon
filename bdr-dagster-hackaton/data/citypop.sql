load spatial;

create table citypop as 
    with source as (
        select
        * exclude geom,
        st_transform(geom, '+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs', 'EPSG:4326') as geom 
        from st_read('data/citypop/CityPOP-ETRS89.shp')
    )
    select
        URAU_CODE,
        population::int as population,
        pop_year::int as population_year,
        city_NAM_1 as city_name,
        st_x(geom) as latitude,
        st_y(geom) as longitude,
        -- st_point(st_x(geom), st_y(geom)) as geom
    from source;

show all tables;
COPY (select * from citypop) TO 'data/citypop.csv';
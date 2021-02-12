import os
import pathlib
import osmnx as ox
import pg_data_etl as pg

from regional_transit_screening_platform import (
    db,
    file_root,
    SQL_DB_NAME,
    DAISY_DB_USER,
    DAISY_DB_PW,
)

# from .scrape_septa_route_statistics import scrape_septa_report


def make_sql_tablename(path: pathlib.Path) -> str:
    """Transform a messy filename into a SQL-compliant table name

    e.g. "My Shapefile.shp" -> "my_shapefile"
    """

    # Force all text to lower-case
    sql_table_name = str(path.name).lower()

    # Strip out the file extension (e.g. '.shp')
    sql_table_name = sql_table_name.replace(path.suffix, "")

    # Replace all instances of any problematic characters
    for character in [" ", "-", "."]:
        if character in sql_table_name:
            sql_table_name = sql_table_name.replace(character, "_")

    return sql_table_name


def import_files():
    """Set up the analysis database:
    1) Create the PostgreSQL database and path to input data
    2) Import all shapefiles
    3) Import all CSVs
    """

    replace_if_exists = {"if_exists": "replace"}

    # 1) Create the project database
    # ------------------------------
    db.create_db()

    input_data_path = file_root / "inputs"

    # 2) Import each input shapefile
    # ------------------------------
    for shp_path in input_data_path.rglob("*.shp"):

        sql_table_name = make_sql_tablename(shp_path)

        print("-" * 80, f"\nImporting raw.{sql_table_name} from {shp_path}")

        db.import_geo_file(shp_path, f"raw.{sql_table_name}", gpd_kwargs=replace_if_exists)

    # 3) Import each input CSV
    # ------------------------

    for csv_path in input_data_path.rglob("*.csv"):

        sql_table_name = make_sql_tablename(csv_path)

        print("-" * 80, f"\nImporting raw.{sql_table_name} from {csv_path}")

        db.import_tabular_file(
            csv_path, f"raw.{sql_table_name}", df_import_kwargs=replace_if_exists
        )


def import_osm():
    """
    Import OpenStreetMap data to the database with osmnx.
    This bounding box overshoots the region and takes a bit to run.
    """

    print("-" * 80, "\nIMPORTING OpenStreetMap DATA")

    north, south, east, west = 40.601963, 39.478606, -73.885803, -76.210785

    print("\t -> Beginning to download...")
    G = ox.graph_from_bbox(north, south, east, west, network_type="drive")
    print("\t -> ... download complete")

    # Force the graph to undirected, which removes duplicate edges
    print("\t -> Forcing graph to undirected edges")
    G = G.to_undirected()

    # Convert to geodataframes and save to DB
    print("\t -> Converting graph to geodataframes")
    nodes, edges = ox.graph_to_gdfs(G)

    edges = edges.to_crs(epsg=26918)

    sql_tablename = "osm_edges_drive"

    db.import_geodataframe(edges, sql_tablename)

    # Make sure uuid extension is available
    db.execute_via_psycopg2('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

    # Make a uuid column
    make_id_query = f"""
        alter table {sql_tablename} add column osmuuid uuid;

        update {sql_tablename} set osmuuid = uuid_generate_v4();
    """
    db.execute_via_psycopg2(make_id_query)


def import_from_daisy_db():
    """
    Pipe data directly from the GTFS database on 'daisy'
    """

    daisy_db = pg.Database("GTFS", **pg.connections["daisy"])

    # Tables to copy
    # (table name, spatial_update_needed)
    tables = [
        ("bus_ridership_spring2019", True),
        ("trolley_ridership_spring2018", True),
        ("lineroutes", False),
        ("stoppoints", False),
        ('"2015base_link"', False),
    ]

    for tbl, spatial_update_needed in tables:

        # Pipe data from 'daisy' via pg_dump directly into analysis db on localhost
        daisy_db.copy_table_to_another_db(tbl, db)

        sql_updates = []

        # The spatial tables have an undefined SRID. This sets is properly.
        if spatial_update_needed:
            sql_define_srid = f"SELECT UpdateGeometrySRID('{tbl}', 'geom', 4326)"
            sql_updates.append(sql_define_srid)

            sql_update_srid = f"SELECT UpdateGeometrySRID('{tbl}', 'geom', 26918)"
            sql_updates.append(sql_update_srid)

        # Each table needs to be moved from public to the 'raw' schema
        query_update_schema = f"ALTER TABLE {tbl} SET SCHEMA raw;"
        sql_updates.append(query_update_schema)

        # SQL tables shouldn't start with numbers. Rename the model links table
        if tbl == '"2015base_link"':
            query_rename_table = 'ALTER TABLE raw."2015base_link" RENAME TO model_2015base_link;'
            sql_updates.append(query_rename_table)

        # Execute the SQL updates in the local analysis database
        for sql_cmd in sql_updates:
            db.execute_via_psycopg2(sql_cmd)


def feature_engineering(
    speed_input: str = "linkspeed_byline",
    speed_mode_input: str = "linkspeedbylinenamecode",
    septa_ridership_input: str = "passloads_segmentlevel_2020_07",
    njt_ridership_input: str = "statsbyline_allgeom",
):
    """
    For all input datasets:
        Filter, rename, add necessary columns (/etc.) as needed.
    """

    default_kwargs = {"geom_type": "LINESTRING", "epsg": 26918}

    for schema in ["speed", "ridership"]:
        db.add_schema(schema)

    # Project any spatial layers that aren't in epsg:26918
    query = "select concat(f_table_schema, '.', f_table_name), srid, type from geometry_columns where srid != 26918"
    for table_to_project in db.query_via_psycopg2(query):
        tbl, srid, geom_type = table_to_project
        db.project_spatial_table(tbl, srid, 26918, geom_type)

    # Define names of the tables that we'll create
    sql_tbl = {
        "speed": "speed.rtsp_input_speed",
        "ridership_septa": "ridership.rtsp_input_ridership_septa",
        "ridership_njt": "ridership.rtsp_input_ridership_njt",
    }

    # AVERAGE SPEED BY SEGMENT
    # ------------------------

    # Isolate features that are:
    #   - surface transit >>> "tsyscode in ('Bus', 'Trl')"
    #   - speed is not null and more than 0
    speed_query = f"""
        select
            t.tsyscode,
            g.*
        from raw.{speed_input} g
        left join
            raw.{speed_mode_input} t
            on
                g.linename = t.linename
        where
            tsyscode in ('Bus', 'Trl')
        and
            avgspeed is not null
        and
            avgspeed > 0
    """
    db.make_geotable_from_query(speed_query, sql_tbl["speed"], **default_kwargs)

    # Make a new speed column that forces values over 75 down to 75
    query_over75 = f"""
        alter table {sql_tbl['speed']} drop column if exists speed;
        alter table {sql_tbl['speed']} add column speed float;

        update {sql_tbl['speed']} set speed = (
            case when avgspeed < 75 then avgspeed else 75 end
        );
    """
    db.execute_via_psycopg2(query_over75)

    # SEPTA RIDERSHIP
    # ---------------

    # Filter out ridership segments that don't have volumes
    septa_query = f"""
        SELECT * FROM raw.{septa_ridership_input}
        WHERE round IS NOT NULL and round > 0;
    """
    db.make_geotable_from_query(septa_query, sql_tbl["ridership_septa"], **default_kwargs)

    # NJT RIDERSHIP
    # -------------

    # Select NJT routes with at least 1 rider
    njt_query = f"""
        SELECT * FROM raw.{njt_ridership_input} t
        WHERE t.name LIKE 'njt%%' AND dailyrider > 0;
    """
    njt_kwargs = {"geom_type": "MULTILINESTRING", "epsg": 26918}
    db.make_geotable_from_query(njt_query, sql_tbl["ridership_njt"], **njt_kwargs)


if __name__ == "__main__":
    import_files()
    # import_osm()
    # feature_engineering()
    # scrape_septa_report()

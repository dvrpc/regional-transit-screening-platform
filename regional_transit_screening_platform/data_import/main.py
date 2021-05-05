import pathlib
import osmnx as ox
from rich import print

from pg_data_etl import Database

from regional_transit_screening_platform import FILE_ROOT, db_connection

db = db_connection()

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
    db.admin("create")

    input_data_path = FILE_ROOT / "inputs"

    # 2) Import each input shapefile
    # ------------------------------
    for shp_path in input_data_path.rglob("*.shp"):

        sql_table_name = make_sql_tablename(shp_path)

        print("-" * 80, f"\nImporting raw.{sql_table_name} from {shp_path}")

        db.import_gis(
            filepath=shp_path, sql_tablename=f"raw.{sql_table_name}", gpd_kwargs=replace_if_exists
        )

    # 3) Import each input CSV
    # ------------------------

    for csv_path in input_data_path.rglob("*.csv"):

        sql_table_name = make_sql_tablename(csv_path)

        print("-" * 80, f"\nImporting raw.{sql_table_name} from {csv_path}")

        db.import_file_with_pandas(
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
    db.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

    # Make a uuid column
    make_id_query = f"""
        alter table {sql_tablename} add column osmuuid uuid;

        update {sql_tablename} set osmuuid = uuid_generate_v4();
    """
    db.execute(make_id_query)


def import_from_daisy_db():
    """
    Pipe data directly from the GTFS database on 'daisy'
    """

    daisy_db = Database.from_config(
        "gtfs_from_daisy",
        "localhost",
        bin_paths={"psql": "/Applications/Postgres.app/Contents/Versions/latest/bin"},
    )

    # Setting this prevents version conflicts: geopandas installs its own pg_dump
    # pg_dump is not installed in base, so this uses the system pg_dump

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
        daisy_db.export_table_to_another_db(tbl, db)

        sql_updates = []

        # Some spatial tables have an undefined SRID of 4326.
        # This sets is properly to 26918
        if spatial_update_needed:
            db.gis_table_update_spatial_data_projection(tbl, 4326, 26918, "POINT")

        # Each table needs to be moved from public to the 'raw' schema
        query_update_schema = f"ALTER TABLE {tbl} SET SCHEMA raw;"
        sql_updates.append(query_update_schema)

        # SQL tables shouldn't start with numbers. Rename the model links table
        if tbl == '"2015base_link"':
            query_rename_table = 'ALTER TABLE raw."2015base_link" RENAME TO model_2015base_link;'
            sql_updates.append(query_rename_table)

        # Execute the SQL updates in the local analysis database
        for sql_cmd in sql_updates:
            print("\t----", sql_cmd)
            db.execute(sql_cmd)


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
        db.schema_add(schema)

    # Project any spatial layers that aren't in epsg:26918
    query = """
        select
            concat(f_table_schema, '.', f_table_name),
            srid,
            type
        from
            geometry_columns
        where
            srid != 26918
    """
    for table_to_project in db.query(query):
        tbl, srid, geom_type = table_to_project
        db.gis_table_update_spatial_data_projection(tbl, srid, 26918, geom_type)

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
    db.gis_make_geotable_from_query(speed_query, sql_tbl["speed"], **default_kwargs)

    # Make a new speed column that forces values over 75 down to 75
    query_over75 = f"""
        alter table {sql_tbl['speed']} drop column if exists speed;
        alter table {sql_tbl['speed']} add column speed float;

        update {sql_tbl['speed']} set speed = (
            case when avgspeed < 75 then avgspeed else 75 end
        );
    """
    db.execute(query_over75)

    # SEPTA RIDERSHIP
    # ---------------

    # Filter out ridership segments that don't have volumes
    septa_query = f"""
        SELECT * FROM raw.{septa_ridership_input}
        WHERE round IS NOT NULL and round > 0;
    """
    db.gis_make_geotable_from_query(septa_query, sql_tbl["ridership_septa"], **default_kwargs)

    # NJT RIDERSHIP
    # -------------

    # Select NJT routes with at least 1 rider
    njt_query = f"""
        SELECT * FROM raw.{njt_ridership_input} t
        WHERE t.name LIKE 'njt%%' AND dailyrider > 0;
    """
    njt_kwargs = {"geom_type": "MULTILINESTRING", "epsg": 26918}
    db.gis_make_geotable_from_query(njt_query, sql_tbl["ridership_njt"], **njt_kwargs)


if __name__ == "__main__":
    import_files()
    # import_osm()
    # feature_engineering()
    # scrape_septa_report()

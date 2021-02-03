import pathlib
import osmnx as ox

from regional_transit_screening_platform import db, file_root

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

    # 1) Create the project database
    # ------------------------------
    db.db_create()

    input_data_path = file_root / "inputs"

    # 2) Import each input shapefile
    # ------------------------------
    for shp_path in input_data_path.rglob("*.shp"):

        sql_table_name = make_sql_tablename(shp_path)
        db.import_geodata(
            table_name=sql_table_name, data_path=shp_path, if_exists="replace", schema="raw"
        )

    # 3) Import each input CSV
    # ------------------------

    for csv_path in input_data_path.rglob("*.csv"):

        sql_table_name = make_sql_tablename(csv_path)
        db.import_csv(
            table_name=sql_table_name, csv_path=csv_path, if_exists="replace", schema="raw"
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

    db.import_geodataframe(edges, "osm_edges")

    # Reproject from 4326 to 26918 to facilitate analysis queries
    db.table_reproject_spatial_data("osm_edges", 4326, 26918, "LINESTRING")

    # Make a uuid column
    make_id_query = """
        alter table osm_edges add column osmuuid uuid;

        update osm_edges set osmuuid = uuid_generate_v4();
    """
    db.execute(make_id_query)


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

    # Define names of the tables that we'll create
    sql_tbl = {
        "speed": "rtsp_input_speed",
        "ridership_septa": "rtsp_input_ridership_septa",
        "ridership_njt": "rtsp_input_ridership_njt",
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
        from {speed_input} g
        left join
            {speed_mode_input} t
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
    db.execute(query_over75)

    # SEPTA RIDERSHIP
    # ---------------

    # Filter out ridership segments that don't have volumes
    septa_query = f"""
        SELECT * FROM {septa_ridership_input}
        WHERE round IS NOT NULL and round > 0;
    """
    db.make_geotable_from_query(septa_query, sql_tbl["ridership_septa"], **default_kwargs)

    # NJT RIDERSHIP
    # -------------

    # Select NJT routes with at least 1 rider
    njt_query = f"""
        SELECT * FROM {njt_ridership_input} t
        WHERE t.name LIKE 'njt%%' AND dailyrider > 0;
    """
    db.make_geotable_from_query(njt_query, sql_tbl["ridership_njt"], **default_kwargs)


if __name__ == "__main__":
    import_files()
    # import_osm()
    # feature_engineering()
    # scrape_septa_report()

import pathlib
import osmnx as ox

from regional_transit_screening_platform import db, file_root


def make_sql_tablename(path: pathlib.Path) -> str:
    """ Transform a messy filename into a SQL-compliant table name

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
    """ Set up the analysis database:
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
            table_name=sql_table_name,
            data_path=shp_path,
            if_exists="replace"
        )

    # 3) Import each input CSV
    # ------------------------

    for csv_path in input_data_path.rglob("*.csv"):

        sql_table_name = make_sql_tablename(csv_path)
        db.import_csv(
            table_name=sql_table_name,
            csv_path=csv_path,
            if_exists="replace"
        )


def import_osm():
    """
        Import OpenStreetMap data to the database with osmnx.
        This bounding box overshoots the region and takes a bit to run.
    """

    print("-" * 80, "\nIMPORTING OpenStreetMap DATA")

    north, south, east, west = 40.601963, 39.478606, -73.885803, -76.210785

    print("\t -> Beginning to download...")
    G = ox.graph_from_bbox(north, south, east, west, network_type='drive')
    print("\t -> ... download complete")

    # Force the graph to undirected, which removes duplicate edges
    print("\t -> Forcing graph to undirected edges")
    G = G.to_undirected()

    # Convert to geodataframes and save to DB
    print("\t -> Converting graph to geodataframes")
    nodes, edges = ox.graph_to_gdfs(G)

    db.import_geodataframe(edges, "osm_edges")


if __name__ == "__main__":
    import_files()
    import_osm()

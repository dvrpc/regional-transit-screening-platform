import pathlib
from regional_transit_screening_platform import db, file_root


def make_sql_tablename(path: pathlib.Path) -> str:
    """ Transform a messy filename into a SQL-compliant table name

        e.g. "My Shapefile.shp" -> "my_shapefile"
    """

    # Force all text to lower-case
    sql_table_name = path.lower()

    # Strip out the file extension (e.g. '.shp')
    sql_table_name = sql_table_name.replace(path.suffix, "")

    # Replace all instances of any problematic characters
    for character in [" ", "-", "."]:
        if character in sql_table_name:
            sql_table_name = sql_table_name.replace(character, "_")

    return sql_table_name


def main():
    """ Set up the analysis database:
        1) Create the PostgreSQL database and path to input data
        2) Import all shapefiles
        3) Import all CSVs
    """

    # 1) Create the project database
    db.db_create()

    input_data_path = file_root / "inputs"

    # 2) Import each input shapefile
    for shp_path in input_data_path.rglob("*.shp"):

        sql_table_name = make_sql_tablename(shp_path)
        db.import_geodata(sql_table_name, shp_path, if_exists="replace")

    # 3) Import each input CSV
    for csv_path in input_data_path.rglob("*.csv"):

        sql_table_name = make_sql_tablename(csv_path)
        db.import_csv(csv_path, if_exists="replace")


if __name__ == "__main__":
    main()

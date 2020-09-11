"""
This is a 'lightweight' implementation of the code PostgreSQL class
defined within https://github.com/aaronfraint/postgis-helpers

It is included here (instead of listed as a dependency) to keep this
project as stand-alone as possible, and also to serve as a teaching
tool for DVRPC team members.
"""
from datetime import datetime

import psycopg2
import sqlalchemy
from geoalchemy2 import Geometry, WKTElement

import pandas as pd
import geopandas as gpd

from typing import Union
from pathlib import Path


class PostgreSQL():
    """
    This class encapsulates interactions with a ``PostgreSQL``
    database. It leverages ``psycopg2``, ``sqlalchemy``, and ``geoalchemy2``
    as needed. It stores connection information that includes:
        - database name
        - username & password
        - host & port
        - superusername & password
        - the SQL cluster's master database
    """

    def __init__(self,
                 working_db: str,
                 un: str = "postgres",
                 pw: str = "this-is-not-my-real-password",
                 host: str = "localhost",
                 port: int = 5432,
                 sslmode: str = None,
                 super_db: str = "postgres",
                 active_schema: str = "public",
                 super_un=None,
                 super_pw=None):

        self.DATABASE = working_db
        self.USER = un
        self.PASSWORD = pw
        self.HOST = host
        self.PORT = port
        self.SSLMODE = sslmode
        self.SUPER_DB = super_db

        if super_un:
            self.SUPER_USER = super_un
        else:
            self.SUPER_USER = un
        if super_pw:
            self.SUPER_PASSWORD = super_pw
        else:
            self.SUPER_PASSWORD = pw

        self.ACTIVE_SCHEMA = active_schema

        if not self.exists():
            print(f"!!! WARNING !!!\n\t--> Database '{working_db}' does not exist on {host}")

    # Helper functions to connect to / create the database
    # ----------------------------------------------------
    def uri(self, super_uri: bool = False) -> str:
        """
        Create a connection string URI for this database.
        :param super_uri: Flag that will provide access to cluster
                          root if True, defaults to False
        :type super_uri: bool, optional
        :return: Connection string URI for PostgreSQL
        :rtype: str
        """

        # If super_uri is True, use the super un/pw/db
        if super_uri:
            user = self.SUPER_USER
            pw = self.SUPER_PASSWORD
            database = self.SUPER_DB

        # Otherwise, use the normal connection info
        else:
            user = self.USER
            pw = self.PASSWORD
            database = self.DATABASE

        connection_string = \
            f"postgresql://{user}:{pw}@{self.HOST}:{self.PORT}/{database}"

        if self.SSLMODE:
            connection_string += f"?sslmode={self.SSLMODE}"

        return connection_string

    def exists(self) -> bool:
        """
        Does this database exist yet? Returns True or False
        :return: True or False if the database exists on the cluster
        :rtype: bool
        """

        sql_db_exists = f"""
            SELECT EXISTS(
                SELECT datname FROM pg_catalog.pg_database
                WHERE lower(datname) = lower('{self.DATABASE}')
            );
        """
        return self.query_as_single_item(sql_db_exists, super_uri=True)

    def db_create(self) -> None:
        """
        Create this database if it doesn't exist yet
        """

        if self.exists():
            print(f"Database {self.DATABASE} already exists")
        else:
            print(f"Creating database: {self.DATABASE} on {self.HOST}")

            sql_make_db = f"CREATE DATABASE {self.DATABASE};"

            self.execute(sql_make_db, autocommit=True)

            # Add PostGIS if not already installed
            if "geometry_columns" in self.all_tables_as_list():
                print("PostGIS comes pre-installed")
            else:
                print("Installing PostGIS")

                sql_add_postgis = "CREATE EXTENSION postgis;"
                self.execute(sql_add_postgis)

    # Make a permanent change to the database
    # ---------------------------------------

    def execute(self,
                query: str,
                autocommit: bool = False):
        """
        Execute a query for a persistent result in the database.
        Use ``autocommit=True`` when creating and deleting databases.

        :param query: any valid SQL query string
        :type query: str
        :param autocommit: flag that will execute against the
                           super db/user, defaults to False
        :type autocommit: bool, optional
        """

        # print("... executing ...\n")

        # if len(query) < 5000:
        #     print(query)

        uri = self.uri(super_uri=autocommit)

        connection = psycopg2.connect(uri)
        if autocommit:
            connection.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )

        cursor = connection.cursor()

        cursor.execute(query)

        cursor.close()
        connection.commit()
        connection.close()

    # Extract data from the database in a variety of formats
    # ------------------------------------------------------

    def query_as_list(self,
                      query: str,
                      super_uri: bool = False) -> list:
        """
        Query the database and get the result as a ``list``
        :param query: any valid SQL query string
        :type query: str
        :param super_uri: flag that will execute against the
                          super db/user, defaults to False
        :type super_uri: bool, optional
        :return: list with each item being a row from the query result
        :rtype: list
        """
        uri = self.uri(super_uri=super_uri)

        connection = psycopg2.connect(uri)

        cursor = connection.cursor()

        cursor.execute(query)

        result = cursor.fetchall()

        cursor.close()
        connection.close()

        return result

    def query_as_df(self,
                    query: str,
                    super_uri: bool = False) -> pd.DataFrame:
        """
        Query the database and get the result as a ``pandas.DataFrame``
        :param query: any valid SQL query string
        :type query: str
        :param super_uri: flag that will execute against the
                          super db/user, defaults to False
        :type super_uri: bool, optional
        :return: dataframe with the query result
        :rtype: pd.DataFrame
        """

        uri = self.uri(super_uri=super_uri)

        engine = sqlalchemy.create_engine(uri)
        df = pd.read_sql(query, engine)
        engine.dispose()

        return df

    def query_as_geo_df(self,
                        query: str,
                        geom_col: str = "geom") -> gpd.GeoDataFrame:
        """
        Query the database and get the result as a ``geopandas.GeoDataFrame``
        :param query: any valid SQL query string
        :type query: str
        :param geom_col: name of the column that holds the geometry,
                         defaults to 'geom'
        :type geom_col: str
        :return: geodataframe with the query result
        :rtype: gpd.GeoDataFrame
        """

        connection = psycopg2.connect(self.uri())

        gdf = gpd.GeoDataFrame.from_postgis(query,
                                            connection,
                                            geom_col=geom_col)

        connection.close()

        return gdf

    def query_as_single_item(self,
                             query: str,
                             super_uri: bool = False):
        """
        Query the database and get the result as a SINGLETON.
        For when you want to transform ``[(True,)]`` into ``True``

        :param query: any valid SQL query string
        :type query: str
        :param super_uri: flag that will execute against the
                          super db/user, defaults to False
        :type super_uri: bool, optional
        :return: result from the query
        :rtype: singleton, type depends on the SQL query
        """

        result = self.query_as_list(query, super_uri=super_uri)

        return result[0][0]

    # IMPORT data into the database
    # -----------------------------

    def import_dataframe(self,
                         dataframe: pd.DataFrame,
                         table_name: str,
                         if_exists: str = "fail",
                         schema: str = None) -> None:
        """
        Import an in-memory ``pandas.DataFrame`` to the SQL database.
        Enforce clean column names (without spaces, caps, or weird symbols).

        :param dataframe: dataframe with data you want to save
        :type dataframe: pd.DataFrame
        :param table_name: name of the table that will get created
        :type table_name: str
        :param if_exists: pandas argument to handle overwriting data,
                          defaults to "fail"
        :type if_exists: str, optional
        """

        if not schema:
            schema = self.ACTIVE_SCHEMA

        print(f"\t -> SQL tablename: {schema}.{table_name}")

        # Replace "Column Name" with "column_name"
        dataframe.columns = dataframe.columns.str.replace(' ', '_')
        dataframe.columns = [x.lower() for x in dataframe.columns]

        # Remove '.' and '-' from column names.
        # i.e. 'geo.display-label' becomes 'geodisplaylabel'
        for s in ['.', '-', '(', ')', '+']:
            dataframe.columns = dataframe.columns.str.replace(s, '')

        # Write to database
        engine = sqlalchemy.create_engine(self.uri())
        dataframe.to_sql(table_name, engine, if_exists=if_exists, schema=schema)
        engine.dispose()

    def import_geodataframe(self,
                            gdf: gpd.GeoDataFrame,
                            table_name: str,
                            src_epsg: Union[int, bool] = False,
                            if_exists: str = "replace",
                            schema: str = None,
                            uid_col: str = "uid"):
        """
        Import an in-memory ``geopandas.GeoDataFrame`` to the SQL database.

        :param gdf: geodataframe with data you want to save
        :type gdf: gpd.GeoDataFrame
        :param table_name: name of the table that will get created
        :type table_name: str
        :param src_epsg: The source EPSG code can be passed as an integer.
                         By default this function will try to read the EPSG
                         code directly, but some spatial data is funky and
                         requires that you explicitly declare its projection.
                         Defaults to False
        :type src_epsg: Union[int, bool], optional
        :param if_exists: pandas argument to handle overwriting data,
                          defaults to "replace"
        :type if_exists: str, optional
        """
        if not schema:
            schema = self.ACTIVE_SCHEMA

        # Read the geometry type. It's possible there are
        # both MULTIPOLYGONS and POLYGONS. This grabs the MULTI variant

        geom_types = list(gdf.geometry.geom_type.unique())
        geom_typ = max(geom_types, key=len).upper()

        print(f"\t -> SQL tablename: {schema}.{table_name}")
        print(f"\t -> Geometry type: {geom_typ}")
        print(f"\t -> Beginning DB import...")
        
        start_time = datetime.now()

        # Manually set the EPSG if the user passes one
        if src_epsg:
            gdf.crs = f"epsg:{src_epsg}"
            epsg_code = src_epsg

        # Otherwise, try to get the EPSG value directly from the geodataframe
        else:
            # Older gdfs have CRS stored as a dict: {'init': 'epsg:4326'}
            if type(gdf.crs) == dict:
                epsg_code = int(gdf.crs['init'].split(" ")[0].split(':')[1])
            # Now geopandas has a different approach
            else:
                epsg_code = int(str(gdf.crs).split(':')[1])

        # Sanitize the columns before writing to the database
        # Make all column names lower case
        gdf.columns = [x.lower() for x in gdf.columns]

        # Replace the 'geom' column with 'geometry'
        if 'geom' in gdf.columns:
            gdf['geometry'] = gdf['geom']
            gdf.drop('geom', 1, inplace=True)

        # Drop the 'gid' column
        if 'gid' in gdf.columns:
            gdf.drop('gid', 1, inplace=True)

        # Rename 'uid' to 'old_uid'
        if uid_col in gdf.columns:
            gdf[f'old_{uid_col}'] = gdf[uid_col]
            gdf.drop(uid_col, 1, inplace=True)

        # Build a 'geom' column using geoalchemy2
        # and drop the source 'geometry' column
        gdf['geom'] = gdf['geometry'].apply(
                                    lambda x: WKTElement(x.wkt, srid=epsg_code)
        )
        gdf.drop('geometry', 1, inplace=True)

        # Write geodataframe to SQL database
        engine = sqlalchemy.create_engine(self.uri())
        gdf.to_sql(table_name,
                   engine,
                   if_exists=if_exists,
                   index=True,
                   index_label='gid',
                   schema=schema,
                   dtype={'geom': Geometry(geom_typ, srid=epsg_code)})
        engine.dispose()

        end_time = datetime.now()

        runtime = end_time - start_time
        print(f"\t -> ... import completed in {runtime}")

        self.table_add_uid_column(table_name, schema=schema, uid_col=uid_col)
        self.table_add_spatial_index(table_name, schema=schema)

    def import_csv(self,
                   table_name: str,
                   csv_path: Path,
                   if_exists: str = "append",
                   schema: str = None,
                   **csv_kwargs):
        r"""
        Load a CSV into a dataframe, then save the df to SQL.
        :param table_name: Name of the table you want to create
        :type table_name: str
        :param csv_path: Path to data. Anything accepted by Pandas works here.
        :type csv_path: Path
        :param if_exists: How to handle overwriting existing data,
                          defaults to ``"append"``
        :type if_exists: str, optional
        :param \**csv_kwargs: any kwargs for ``pd.read_csv()`` are valid here.
        """

        if not schema:
            schema = self.ACTIVE_SCHEMA

        print("-" * 80, "\nLOAD CSV INTO DATABASE")
        print(f"\t -> Reading source file: {csv_path.name}")

        # Read the CSV with whatever kwargs were passed
        df = pd.read_csv(csv_path, **csv_kwargs)

        self.import_dataframe(df, table_name, if_exists=if_exists, schema=schema)

        return df

    def import_geodata(self,
                       table_name: str,
                       data_path: Path,
                       src_epsg: Union[int, bool] = False,
                       if_exists: str = "fail",
                       schema: str = None):
        """
        Load geographic data into a geodataframe, then save to SQL.
        :param table_name: Name of the table you want to create
        :type table_name: str
        :param data_path: Path to the data. Anything accepted by Geopandas
                          works here.
        :type data_path: Path
        :param src_epsg: Manually declare the source EPSG if needed,
                         defaults to False
        :type src_epsg: Union[int, bool], optional
        :param if_exists: pandas argument to handle overwriting data,
                          defaults to "replace"
        :type if_exists: str, optional
        """

        if not schema:
            schema = self.ACTIVE_SCHEMA

        print("-" * 80, "\nLOAD GEODATA INTO DATABASE")
        print(f"\t -> Reading source file: {data_path.name}")

        # Read the data into a geodataframe
        gdf = gpd.read_file(data_path)

        # Drop null geometries
        gdf = gdf[gdf['geometry'].notnull()]

        # Explode multipart to singlepart and reset the index
        gdf = gdf.explode()
        gdf['explode'] = gdf.index
        gdf = gdf.reset_index()

        self.import_geodataframe(gdf,
                                 table_name,
                                 src_epsg=src_epsg,
                                 if_exists=if_exists,
                                 schema=schema)

    # CREATE data within the database
    # -------------------------------

    def make_geotable_from_query(self,
                                 query: str,
                                 new_table_name: str,
                                 geom_type: str,
                                 epsg: int,
                                 schema: str = None,
                                 uid_col: str = "uid") -> None:
        """
        TODO: docstring
        """

        if not schema:
            schema = self.ACTIVE_SCHEMA

        print("-" * 80, "\nMAKE NEW TABLE VIA QUERY")
        print(f"\t -> SQL tablename: {new_table_name}")
        print("\t -> Query: ")
        print(query)

        valid_geom_types = ["POINT", "MULTIPOINT",
                            "POLYGON", "MULTIPOLYGON",
                            "LINESTRING", "MULTILINESTRING"]

        if geom_type.upper() not in valid_geom_types:
            for msg in [
                f"Geometry type of {geom_type} is not valid.",
                f"Please use one of the following: {valid_geom_types}",
                "Aborting"
            ]:
                print(msg)
            return

        sql_make_table_from_query = f"""
            DROP TABLE IF EXISTS {schema}.{new_table_name};
            CREATE TABLE {schema}.{new_table_name} AS
            {query}
        """

        self.execute(sql_make_table_from_query)

        self.table_add_uid_column(new_table_name, schema=schema, uid_col=uid_col)
        self.table_add_spatial_index(new_table_name, schema=schema)
        self.table_reproject_spatial_data(new_table_name,
                                          epsg, epsg,
                                          geom_type=geom_type.upper(),
                                          schema=schema)

    # TABLE-level operations
    # ----------------------

    def table_add_uid_column(self,
                             table_name: str,
                             schema: str = None,
                             uid_col: str = "uid") -> None:
        """
        Add a serial primary key column named 'uid' to the table.
        :param table_name: Name of the table to add a uid column to
        :type table_name: str
        """

        if not schema:
            schema = self.ACTIVE_SCHEMA

        print(f"\t -> Adding uid column")

        sql_unique_id_column = f"""
            ALTER TABLE {schema}.{table_name} DROP COLUMN IF EXISTS {uid_col};
            ALTER TABLE {schema}.{table_name} ADD {uid_col} serial PRIMARY KEY;
        """
        self.execute(sql_unique_id_column)

    def table_add_spatial_index(self, table_name: str, schema: str = None) -> None:
        """
        Add a spatial index to the 'geom' column in the table.
        :param table_name: Name of the table to make the index on
        :type table_name: str
        """

        if not schema:
            schema = self.ACTIVE_SCHEMA

        print(f"\t -> Creating a spatial index")

        sql_make_spatial_index = f"""
            CREATE INDEX ON {schema}.{table_name}
            USING GIST (geom);
        """
        self.execute(sql_make_spatial_index)

    def table_reproject_spatial_data(self,
                                     table_name: str,
                                     old_epsg: Union[int, str],
                                     new_epsg: Union[int, str],
                                     geom_type: str,
                                     schema: str = None) -> None:
        """
        Transform spatial data from one EPSG into another EPSG.
        This can also be used with the same old and new EPSG. This
        is useful when making a new geotable, as this SQL code
        will update the table's entry in the ``geometry_columns`` table.
        :param table_name: name of the table
        :type table_name: str
        :param old_epsg: Current EPSG of the data
        :type old_epsg: Union[int, str]
        :param new_epsg: Desired new EPSG for the data
        :type new_epsg: Union[int, str]
        :param geom_type: PostGIS-valid name of the
                          geometry you're transforming
        :type geom_type: str
        """

        if not schema:
            schema = self.ACTIVE_SCHEMA

        msg = f"Reprojecting {schema}.{table_name} from {old_epsg} to {new_epsg}"
        print(msg)

        sql_transform_geom = f"""
            ALTER TABLE {schema}.{table_name}
            ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg})
            USING ST_Transform( ST_SetSRID( geom, {old_epsg} ), {new_epsg} );
        """
        self.execute(sql_transform_geom)

    # LISTS of things inside this database
    # ------------------------------------

    def all_tables_as_list(self, schema: str = None) -> list:
        """
        Get a list of all tables in the database. 
        Optionally filter to a schema

        :param schema: name of the schema to filter by
        :type schema: str
        :return: List of tables in the database
        :rtype: list
        """

        sql_all_tables = """
            SELECT table_name
            FROM information_schema.tables
        """

        if schema:
            sql_all_tables += f"""
                WHERE table_schema = '{schema}'
        """

        tables = self.query_as_list(sql_all_tables)

        return [t[0] for t in tables]

    def all_spatial_tables_as_dict(self, schema: str = None) -> dict:
        """
        Get a dictionary of all spatial tables in the database.
        Return value is formatted as: ``{table_name: epsg}``

        :return: Dictionary with spatial table names as keys
                 and EPSG codes as values.
        :rtype: dict
        """

        sql_all_spatial_tables = """
            SELECT f_table_name AS tblname, srid
            FROM geometry_columns
        """

        if schema:
            sql_all_spatial_tables += f"""
                WHERE f_table_schema = '{schema}'
        """

        spatial_tables = self.query_as_list(sql_all_spatial_tables)

        return {t[0]: t[1] for t in spatial_tables}

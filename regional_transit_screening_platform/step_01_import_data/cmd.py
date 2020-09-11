"""
Command-Line Interface for the database setup
"""
import click

from .main import import_files, import_osm


@click.command()
def db_setup_from_shp():
    """Create a local SQL db & import .shp and .csv datasets"""
    import_files()


@click.command()
def db_import_osm():
    """Import OpenStreetMap edges to the SQL db"""
    import_osm()

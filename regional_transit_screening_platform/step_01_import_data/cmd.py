"""
Command-Line Interface for the database setup
"""
import click

from .main import main


@click.command()
def setup_db_from_shp():
    """Create a local SQL db & import all input datasets"""
    main()

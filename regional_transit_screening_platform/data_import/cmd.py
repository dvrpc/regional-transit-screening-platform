"""
Command-Line Interface for the database setup
"""
import click

from .main import (
    import_files,
    import_osm,
    feature_engineering,
    # scrape_septa_report
)


@click.command()
def db_import_files():
    """Create a local SQL db & import .shp and .csv datasets"""
    import_files()


@click.command()
def db_import_osm():
    """Import OpenStreetMap edges to the SQL db"""
    import_osm()


@click.command()
def db_feature_engineering():
    """Clean up source data for analysis"""
    feature_engineering()


# @click.command()
# def db_scrape_septa_report():
#     """Scrape SEPTA's annual stats report"""
#     scrape_septa_report()

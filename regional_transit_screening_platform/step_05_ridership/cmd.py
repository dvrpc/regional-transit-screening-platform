"""
Command-Line Interface for the ridership analysis
"""
import click

from .main import match_septa_ridership_with_osm, analyze_ridership


@click.command()
def ridership_match_osm_w_septa():
    """Match ridership segments with OSM features"""
    match_septa_ridership_with_osm()


@click.command()
def ridership_analysis():
    """Calculate an average ridership value for OSM features"""
    analyze_ridership()

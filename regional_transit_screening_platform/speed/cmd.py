"""
CLI for the speed data analysis
"""
import click

from .main import match_speed_features_with_osm, analyze_speed


@click.command()
def speed_match_osm():
    """Match speed segments to OSM features"""
    match_speed_features_with_osm()


@click.command()
def speed_analysis():
    """Calculate a weighted average speed for OSM features"""
    analyze_speed()

"""
This module uses `click` to create a command-line-interface (CLI)
for the `regional_transit_screening_platform`
"""

import click

from regional_transit_screening_platform.data_import import cmd as cmd_data
from regional_transit_screening_platform.speed import cmd as cmd_speed
from regional_transit_screening_platform.ridership import cmd as cmd_ridership


@click.group()
def main():
    """RTSP allows command-line execution of the analysis."""
    pass


# Commands for "data_import" module
main.add_command(cmd_data.db_import_files)
main.add_command(cmd_data.db_import_osm)
main.add_command(cmd_data.db_feature_engineering)

# Commands for the "speed" module
main.add_command(cmd_speed.speed_match_osm)
main.add_command(cmd_speed.speed_analysis)

# Commands for the "ridership" module
main.add_command(cmd_ridership.ridership_match_osm_w_septa)
main.add_command(cmd_ridership.ridership_match_osm_w_njt)
main.add_command(cmd_ridership.ridership_analysis)

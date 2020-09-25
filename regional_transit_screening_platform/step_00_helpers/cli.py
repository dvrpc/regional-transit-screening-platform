"""
This module uses `click` to create a command-line-interface (CLI)
for the `regional_transit_screening_platform`
"""

import click

from regional_transit_screening_platform.step_01_import_data import cmd as cmd_01
from regional_transit_screening_platform.step_02_average_speed import cmd as cmd_02
from regional_transit_screening_platform.step_05_ridership import cmd as cmd_05

@click.group()
def main():
    """RTSP allows command-line execution of the analysis. """
    pass


main.add_command(cmd_01.db_setup_from_shp)
main.add_command(cmd_01.db_import_osm)
main.add_command(cmd_01.db_feature_engineering)
main.add_command(cmd_01.db_scrape_septa_report)
main.add_command(cmd_02.speed_match_osm)
main.add_command(cmd_02.speed_analysis)
main.add_command(cmd_05.ridership_match_osm_w_septa)
main.add_command(cmd_05.ridership_analysis)

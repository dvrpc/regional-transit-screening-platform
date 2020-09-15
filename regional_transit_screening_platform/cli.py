import click

# from sidewalk_gaps.db_setup import commands as db_setup_commands
from regional_transit_screening_platform.step_01_import_data import cmd as cmd_01
from regional_transit_screening_platform.step_02_average_speed import cmd as cmd_02


@click.group()
def main():
    """RTSP allows command-line execution of the analysis. """
    pass


main.add_command(cmd_01.db_setup_from_shp)
main.add_command(cmd_01.db_import_osm)
main.add_command(cmd_02.speed_match_osm)
main.add_command(cmd_02.speed_analysis)

import click

# from sidewalk_gaps.db_setup import commands as db_setup_commands
from regional_transit_screening_platform.step_01_import_data import cmd as cmd_01


@click.group()
def main():
    """RTSP allows command-line execution of
       DVRPC's regional transit analysis scripts. """
    pass


main.add_command(cmd_01.db_setup_from_shp)
main.add_command(cmd_01.db_import_osm)

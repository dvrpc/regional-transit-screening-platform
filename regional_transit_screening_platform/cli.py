import click

# from sidewalk_gaps.db_setup import commands as db_setup_commands

@click.group()
def main():
    """RTSP allows command-line execution of regional transit analysis scripts"""
    pass


# main.add_command(db_setup_commands.db_setup)
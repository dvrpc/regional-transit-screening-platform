import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

from pg_data_etl import Database
from .step_00_helpers.interpolation import match_features_with_osm  # noqa

# Load environment variables
load_dotenv(find_dotenv())
SQL_DB_NAME = os.getenv("SQL_DB_NAME")
FILE_ROOT = Path(os.getenv("GDRIVE_PROJECT_FOLDER"))


# DB_USER = os.getenv("DB_USER")
# DB_PW = os.getenv("DB_PW")
# DAISY_DB_USER = os.getenv("DAISY_DB_USER")
# DAISY_DB_PW = os.getenv("DAISY_DB_PW")


def db_connection():
    return Database.from_config(SQL_DB_NAME, "localhost")

import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

import pg_data_etl as pg

# from .step_00_helpers.database import PostgreSQL

load_dotenv(find_dotenv())
DB_USER = os.getenv("DB_USER")
DB_PW = os.getenv("DB_PW")
SQL_DB_NAME = os.getenv("SQL_DB_NAME")

DAISY_DB_USER = os.getenv("DAISY_DB_USER")
DAISY_DB_PW = os.getenv("DAISY_DB_PW")

GDRIVE_PROJECT_FOLDER = os.getenv("GDRIVE_PROJECT_FOLDER")

# db = PostgreSQL(SQL_DB_NAME, un=DB_USER, pw=DB_PW)
db = pg.Database(SQL_DB_NAME, **pg.connections["localhost"])
file_root = Path(GDRIVE_PROJECT_FOLDER)

# Load up helper functions that require DB to be defined first
from .step_00_helpers.interpolation import match_features_with_osm

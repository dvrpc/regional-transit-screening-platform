import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

from .database import PostgreSQL

load_dotenv(find_dotenv())
DB_USER = os.getenv("DB_USER")
DB_PW = os.getenv("DB_PW")
SQL_DB_NAME = os.getenv("SQL_DB_NAME")
GDRIVE_PROJECT_FOLDER = os.getenv("GDRIVE_PROJECT_FOLDER")

db = PostgreSQL(SQL_DB_NAME)
file_root = Path(GDRIVE_PROJECT_FOLDER)

# Load up helper functions that require DB to be defined first
from .step_00_helpers.interpolation import match_features_with_osm

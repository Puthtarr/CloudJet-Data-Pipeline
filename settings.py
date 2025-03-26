import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Data directiries
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
RAW_FLIGHT_DATA_DIR = RAW_DATA_DIR / "flight"
RAW_WEATHER_DATA_DIR = RAW_DATA_DIR / "weather"
CLEAN_DATA_DIR = DATA_DIR / "clean"
LOGS_DATA_DIR = DATA_DIR / "logs"

# Dags Directories
DAGS_DIR = BASE_DIR / "dags"

# Docker Directories
DOCKER_DIR = BASE_DIR / "docker"

# Scripts Directories
SCRIPTS_DIR = BASE_DIR / "scripts"

# Notebook Directories
NOTEBOOK_DIR = BASE_DIR / "notebook"

all_dir_list = [
    DATA_DIR, RAW_DATA_DIR, RAW_FLIGHT_DATA_DIR, RAW_WEATHER_DATA_DIR,
    CLEAN_DATA_DIR, LOGS_DATA_DIR,
    DAGS_DIR, DOCKER_DIR, SCRIPTS_DIR, NOTEBOOK_DIR
]

# Create folders if not exist
for path in all_dir_list:
    os.makedirs(path, exist_ok=True)


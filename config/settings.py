"""
Configuration constants for Kalshi Quant trading system.
"""
from pathlib import Path

# API Authentication
KEY_ID = "0ac60c80-d575-480e-979b-aa5050a61c1b"
KEY_FILE_PATH = Path("My_First_API_Key.key")  # RSA PEM format private key file

# Database Configuration
DATABASE_PATH = "market_data.duckdb"

# Market Filtering
MIN_DAILY_VOLUME = 100000  # In cents, equals $1000 - API returns volume in cents

# Data Ingestion Configuration
INGESTION_INTERVAL_SECONDS = 60  # How often to fetch and store snapshots
NUM_FED_MEETINGS = 4  # Number of upcoming Fed meetings to track


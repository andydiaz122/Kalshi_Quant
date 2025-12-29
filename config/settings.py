"""
Configuration constants for Kalshi Quant trading system.

Environment Variables Required:
    KALSHI_API_KEY_ID: Your Kalshi API Key ID (UUID format)
    KALSHI_KEY_FILE_PATH: Path to RSA private key file (optional, defaults to My_First_API_Key.key)
"""
import os
from pathlib import Path

# API Authentication - Load from environment variables
KEY_ID = os.environ.get("KALSHI_API_KEY_ID", "")
if not KEY_ID:
    raise EnvironmentError(
        "KALSHI_API_KEY_ID environment variable not set. "
        "Please set it to your Kalshi API Key ID."
    )

KEY_FILE_PATH = Path(os.environ.get("KALSHI_KEY_FILE_PATH", "My_First_API_Key.key"))

# Database Configuration
DATABASE_PATH = "market_data.duckdb"

# Market Filtering
MIN_DAILY_VOLUME = 100000  # In cents, equals $1000 - API returns volume in cents

# Data Ingestion Configuration
INGESTION_INTERVAL_SECONDS = 60  # How often to fetch and store snapshots
NUM_FED_MEETINGS = 4  # Number of upcoming Fed meetings to track


"""
QETE Configuration Module

Centralized configuration for the Quantitative Event Trading Engine.
All API credentials, paths, and trading constants are managed here.

Environment Variables:
    KALSHI_API_KEY_ID: Your Kalshi API Key ID (UUID format) - REQUIRED
    KALSHI_KEY_FILE_PATH: Path to RSA private key file (optional)
"""

import os
import warnings
from pathlib import Path

# ==============================================================================
# PATH CONFIGURATION
# ==============================================================================

# Project root (parent of kalshi_qete/)
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# QETE package root
QETE_ROOT = Path(__file__).parent.resolve()

# Data and logs directories
DATA_DIR = QETE_ROOT / "data"
LOGS_DIR = QETE_ROOT / "logs"

# ==============================================================================
# API CREDENTIALS
# ==============================================================================
# Load from environment variables for security
# Set these before running:
#   export KALSHI_API_KEY_ID='your-api-key-id'
#   export KALSHI_KEY_FILE_PATH='/path/to/key.key'  (optional)

KEY_ID = os.environ.get("KALSHI_API_KEY_ID", "")
if not KEY_ID:
    warnings.warn(
        "KALSHI_API_KEY_ID environment variable not set. "
        "API calls will fail until this is configured.",
        UserWarning
    )

# RSA private key file path (relative to project root)
KEY_FILE_PATH = Path(os.environ.get("KALSHI_KEY_FILE_PATH", str(PROJECT_ROOT / "My_First_API_Key.key")))

# ==============================================================================
# TRADING CONFIGURATION
# ==============================================================================

# Minimum 24-hour volume in cents to consider a market liquid
# $1000 = 100,000 cents
MIN_DAILY_VOLUME = 100_000

# Default series to monitor
DEFAULT_SERIES = [
    "KXFEDDECISION",  # Federal Reserve rate decisions
]

# ==============================================================================
# DATABASE CONFIGURATION
# ==============================================================================

# DuckDB database file path
DB_PATH = DATA_DIR / "qete.duckdb"

# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================

# Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = "INFO"

# Log format
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

# ==============================================================================
# PERFORMANCE TUNING
# ==============================================================================

# Number of worker threads for parallel operations
NUM_WORKERS = 4

# API request timeout in seconds
REQUEST_TIMEOUT = 30

# Rate limiting - max requests per second
MAX_REQUESTS_PER_SECOND = 10


"""
QETE - Quantitative Event Trading Engine

A medium-frequency trading system for Kalshi prediction markets.
"""

__version__ = "0.1.0"
__author__ = "Andrew Diaz"

from pathlib import Path

# Package root directory
PACKAGE_ROOT = Path(__file__).parent.resolve()
DATA_DIR = PACKAGE_ROOT / "data"
LOGS_DIR = PACKAGE_ROOT / "logs"


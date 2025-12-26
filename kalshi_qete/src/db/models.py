"""
QETE Data Models

This module defines the data structures used throughout the trading engine.
We use a dual approach:
  1. Pydantic models for validation and type safety at API boundaries
  2. Polars schemas for efficient columnar storage and analytics

Why Polars over Pandas?
  - 10-100x faster for large datasets
  - Zero-copy operations where possible
  - Native lazy evaluation for query optimization
  - Memory-efficient columnar format (matches DuckDB)
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

import polars as pl


# =============================================================================
# POLARS SCHEMAS
# =============================================================================
# These define the column types for our DataFrames.
# Using explicit schemas prevents type inference overhead and ensures consistency.

ORDERBOOK_SNAPSHOT_SCHEMA = {
    "snapshot_ts": pl.Datetime("us"),      # Microsecond precision timestamp
    "ticker": pl.Utf8,                      # Market ticker (e.g., "KXFEDDECISION-26JAN-H0")
    "series_ticker": pl.Utf8,               # Series (e.g., "KXFEDDECISION")
    "market_title": pl.Utf8,                # Human-readable title
    "best_yes_bid": pl.Float64,             # Best Yes bid in cents
    "best_yes_ask": pl.Float64,             # Best Yes ask in cents (or implied)
    "best_no_bid": pl.Float64,              # Best No bid in cents
    "best_no_ask": pl.Float64,              # Best No ask in cents (or implied)
    "yes_spread": pl.Float64,               # Yes ask - Yes bid
    "no_spread": pl.Float64,                # No ask - No bid
    "volume_24h": pl.Int64,                 # 24-hour volume in cents
    "yes_bid_depth": pl.Int64,              # Total quantity at yes bids
    "no_bid_depth": pl.Int64,               # Total quantity at no bids
}

# Schema for raw orderbook levels (for full depth analysis)
ORDERBOOK_LEVEL_SCHEMA = {
    "snapshot_ts": pl.Datetime("us"),
    "ticker": pl.Utf8,
    "side": pl.Utf8,                        # "yes" or "no"
    "price": pl.Float64,                    # Price in cents
    "quantity": pl.Int64,                   # Number of contracts
    "level": pl.Int32,                      # 0 = best, 1 = second best, etc.
}

# Schema for market metadata (slower-changing data)
MARKET_METADATA_SCHEMA = {
    "ticker": pl.Utf8,
    "series_ticker": pl.Utf8,
    "title": pl.Utf8,
    "status": pl.Utf8,                      # "open", "closed", "settled"
    "expiration_ts": pl.Datetime("us"),
    "open_interest": pl.Int64,
    "last_updated": pl.Datetime("us"),
}


# =============================================================================
# PYTHON DATA CLASSES
# =============================================================================
# These provide type-safe structures for passing data between functions.
# Lighter weight than Pydantic when we don't need validation.

@dataclass
class MarketPricing:
    """
    Extracted bid/ask prices from an orderbook.
    
    The "Implied Ask" rule for Kalshi:
    - API only returns bids for Yes and bids for No
    - Yes Ask = 100 - Best No Bid
    - No Ask = 100 - Best Yes Bid
    
    Example:
        If Best_Yes_Bid = 45¢ and Best_No_Bid = 52¢:
        - Implied_Yes_Ask = 100 - 52 = 48¢
        - Implied_No_Ask = 100 - 45 = 55¢
        - Yes_Spread = 48 - 45 = 3¢
    """
    best_yes_bid: float
    best_no_bid: float
    best_yes_ask: Optional[float] = None
    best_no_ask: Optional[float] = None
    yes_spread: Optional[float] = None
    no_spread: Optional[float] = None
    yes_bid_depth: int = 0
    no_bid_depth: int = 0
    
    def calculate_implied_asks(self) -> None:
        """Apply the implied ask rule if asks are missing."""
        if self.best_yes_ask is None and self.best_no_bid is not None:
            self.best_yes_ask = 100.0 - self.best_no_bid
        if self.best_no_ask is None and self.best_yes_bid is not None:
            self.best_no_ask = 100.0 - self.best_yes_bid
    
    def calculate_spreads(self) -> None:
        """Calculate bid-ask spreads."""
        if self.best_yes_ask is not None:
            self.yes_spread = self.best_yes_ask - self.best_yes_bid
        if self.best_no_ask is not None:
            self.no_spread = self.best_no_ask - self.best_no_bid


@dataclass
class OrderbookSnapshot:
    """
    Complete orderbook snapshot for storage.
    
    This is the primary record type stored in DuckDB.
    Each snapshot captures the market state at a point in time.
    """
    snapshot_ts: datetime
    ticker: str
    best_yes_bid: float
    best_no_bid: float
    series_ticker: Optional[str] = None
    market_title: Optional[str] = None
    best_yes_ask: Optional[float] = None
    best_no_ask: Optional[float] = None
    yes_spread: Optional[float] = None
    no_spread: Optional[float] = None
    volume_24h: Optional[int] = None
    yes_bid_depth: Optional[int] = None
    no_bid_depth: Optional[int] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame construction."""
        return {
            "snapshot_ts": self.snapshot_ts,
            "ticker": self.ticker,
            "series_ticker": self.series_ticker,
            "market_title": self.market_title,
            "best_yes_bid": self.best_yes_bid,
            "best_yes_ask": self.best_yes_ask,
            "best_no_bid": self.best_no_bid,
            "best_no_ask": self.best_no_ask,
            "yes_spread": self.yes_spread,
            "no_spread": self.no_spread,
            "volume_24h": self.volume_24h,
            "yes_bid_depth": self.yes_bid_depth,
            "no_bid_depth": self.no_bid_depth,
        }


@dataclass
class MarketInfo:
    """
    Market metadata from the Kalshi API.
    
    Contains slower-changing data about a market (title, status, etc.)
    that doesn't need to be captured with every orderbook snapshot.
    """
    ticker: str
    series_ticker: str
    title: str
    status: str
    volume_24h: int
    event_ticker: Optional[str] = None
    open_interest: Optional[int] = None
    expiration_time: Optional[datetime] = None


# =============================================================================
# CONVERSION UTILITIES
# =============================================================================

def snapshots_to_polars(snapshots: List[OrderbookSnapshot]) -> pl.DataFrame:
    """
    Convert a list of OrderbookSnapshot objects to a Polars DataFrame.
    
    Uses the predefined schema for type consistency.
    
    Args:
        snapshots: List of OrderbookSnapshot dataclass instances
        
    Returns:
        Polars DataFrame with ORDERBOOK_SNAPSHOT_SCHEMA types
        
    Example:
        >>> snapshots = [snap1, snap2, snap3]
        >>> df = snapshots_to_polars(snapshots)
        >>> df.shape
        (3, 13)
    """
    if not snapshots:
        # Return empty DataFrame with correct schema
        return pl.DataFrame(schema=ORDERBOOK_SNAPSHOT_SCHEMA)
    
    # Convert to list of dicts
    data = [s.to_dict() for s in snapshots]
    
    # Create DataFrame with explicit schema
    return pl.DataFrame(data, schema=ORDERBOOK_SNAPSHOT_SCHEMA)


def polars_to_snapshots(df: pl.DataFrame) -> List[OrderbookSnapshot]:
    """
    Convert a Polars DataFrame back to OrderbookSnapshot objects.
    
    Useful when you need to pass data to functions expecting dataclasses.
    
    Args:
        df: Polars DataFrame with orderbook data
        
    Returns:
        List of OrderbookSnapshot dataclass instances
    """
    snapshots = []
    for row in df.iter_rows(named=True):
        snapshots.append(OrderbookSnapshot(
            snapshot_ts=row["snapshot_ts"],
            ticker=row["ticker"],
            series_ticker=row.get("series_ticker"),
            market_title=row.get("market_title"),
            best_yes_bid=row["best_yes_bid"],
            best_yes_ask=row.get("best_yes_ask"),
            best_no_bid=row["best_no_bid"],
            best_no_ask=row.get("best_no_ask"),
            yes_spread=row.get("yes_spread"),
            no_spread=row.get("no_spread"),
            volume_24h=row.get("volume_24h"),
            yes_bid_depth=row.get("yes_bid_depth"),
            no_bid_depth=row.get("no_bid_depth"),
        ))
    return snapshots


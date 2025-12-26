"""
Data models for market pricing and orderbook snapshots.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class MarketPricing(BaseModel):
    """Best bid/ask prices and spreads extracted from orderbook data."""
    
    best_yes_bid: float = Field(description="Best Yes bid price in cents")
    best_yes_ask: Optional[float] = Field(default=None, description="Best Yes ask price in cents")
    best_no_bid: float = Field(description="Best No bid price in cents")
    best_no_ask: Optional[float] = Field(default=None, description="Best No ask price in cents")
    yes_spread: Optional[float] = Field(default=None, description="Yes spread (ask - bid) in cents")
    no_spread: Optional[float] = Field(default=None, description="No spread (ask - bid) in cents")
    
    def calculate_spreads(self) -> None:
        """Calculate spreads from bid/ask prices."""
        if self.best_yes_ask is not None:
            self.yes_spread = self.best_yes_ask - self.best_yes_bid
        if self.best_no_ask is not None:
            self.no_spread = self.best_no_ask - self.best_no_bid


class OrderbookSnapshot(BaseModel):
    """Full orderbook snapshot record for database storage."""
    
    snapshot_timestamp: datetime = Field(description="Timestamp when snapshot was taken")
    ticker: str = Field(description="Market ticker")
    market_title: Optional[str] = Field(default=None, description="Market title")
    series_ticker: Optional[str] = Field(default=None, description="Series ticker (e.g., KXFEDDECISION)")
    best_yes_bid: float = Field(description="Best Yes bid price in cents")
    best_yes_ask: Optional[float] = Field(default=None, description="Best Yes ask price in cents")
    best_no_bid: float = Field(description="Best No bid price in cents")
    best_no_ask: Optional[float] = Field(default=None, description="Best No ask price in cents")
    yes_spread: Optional[float] = Field(default=None, description="Yes spread in cents")
    no_spread: Optional[float] = Field(default=None, description="No spread in cents")
    volume_24h: Optional[int] = Field(default=None, description="24-hour volume in cents")


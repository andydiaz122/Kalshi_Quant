"""
Kalshi API Adapter

A clean wrapper around the kalshi-python SDK that:
1. Handles authentication setup
2. Provides a simplified interface for common operations
3. Handles pagination automatically
4. Converts API responses to our internal data models

This adapter isolates the rest of the codebase from SDK-specific details,
making it easier to adapt if the SDK changes or we need to mock for testing.

Note: The SDK has a bug where it expects 'true'/'false' keys in orderbook
responses, but the API returns 'yes'/'no'. We use raw HTTP for orderbook calls.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Union

import requests

from kalshi_python import (
    ApiClient,
    Configuration,
    MarketsApi,
    ExchangeApi,
)
from kalshi_python.models import Market

from kalshi_qete.src.db.models import MarketInfo, MarketPricing


@dataclass
class OrderbookRaw:
    """
    Raw orderbook data from API before parsing.
    
    Stores the bid arrays exactly as returned by Kalshi.
    Each array contains [price, quantity] pairs sorted low-to-high.
    """
    yes_bids: List[List[int]]  # [[price, qty], ...] sorted low→high
    no_bids: List[List[int]]   # [[price, qty], ...] sorted low→high
    ticker: str
    timestamp: datetime


class KalshiAdapter:
    """
    Adapter for Kalshi API operations.
    
    Encapsulates authentication and provides clean methods for:
    - Fetching markets with filtering
    - Getting orderbook data
    - Checking exchange status
    
    Example:
        >>> adapter = KalshiAdapter(key_id, key_path)
        >>> markets = adapter.get_markets_by_series("KXFEDDECISION", min_volume=100000)
        >>> for market in markets:
        ...     orderbook = adapter.get_orderbook(market.ticker)
    """
    
    def __init__(self, key_id: str, key_file_path: Union[str, Path]):
        """
        Initialize adapter with API credentials.
        
        Args:
            key_id: Kalshi API Key ID (UUID format)
            key_file_path: Path to RSA private key file (.key)
        """
        self.key_id = key_id
        self.key_file_path = Path(key_file_path)
        
        # Validate key file exists
        self._validate_key_file()
        
        # Initialize API client with auth
        self._client = self._create_client()
        
        # Initialize API interfaces
        self._markets_api = MarketsApi(self._client)
        self._exchange_api = ExchangeApi(self._client)
    
    def _validate_key_file(self) -> None:
        """Validate that the key file exists and is valid PEM format."""
        if not self.key_file_path.exists():
            raise FileNotFoundError(
                f"Key file not found: {self.key_file_path}"
            )
        
        # Quick check that it's a valid PEM file
        with open(self.key_file_path, "r", encoding="utf-8") as f:
            key = f.read()
        
        if "-----BEGIN" not in key:
            raise ValueError(f"Invalid PEM format in {self.key_file_path}")
    
    def _create_client(self) -> ApiClient:
        """Create authenticated API client."""
        config = Configuration()
        client = ApiClient(configuration=config)
        
        # Set up Kalshi authentication
        # Note: SDK expects the FILE PATH, not the key content
        client.set_kalshi_auth(self.key_id, str(self.key_file_path))
        
        return client
    
    # =========================================================================
    # EXCHANGE OPERATIONS
    # =========================================================================
    
    def get_exchange_status(self) -> dict:
        """
        Check if the exchange is open for trading.
        
        Returns:
            Dict with 'exchange_active' and 'trading_active' booleans
        """
        response = self._exchange_api.get_exchange_status()
        return {
            "exchange_active": response.exchange_active,
            "trading_active": response.trading_active,
        }
    
    def is_exchange_open(self) -> bool:
        """Check if exchange is currently open for trading."""
        status = self.get_exchange_status()
        return status.get("trading_active", False)
    
    # =========================================================================
    # MARKET OPERATIONS
    # =========================================================================
    
    def get_markets_by_series(
        self,
        series_ticker: str,
        min_volume: int = 0,
        status: str = "open",
        limit: int = 1000
    ) -> List[MarketInfo]:
        """
        Fetch all markets in a series with optional volume filtering.
        
        Handles pagination automatically to get all results.
        
        Args:
            series_ticker: Series to filter by (e.g., "KXFEDDECISION")
            min_volume: Minimum 24h volume in cents (default: 0 = no filter)
            status: Market status filter (default: "open")
            limit: Max results per page (default: 1000)
            
        Returns:
            List of MarketInfo objects meeting the criteria
        """
        all_markets = []
        cursor = None
        
        while True:
            # Fetch page of markets
            response = self._markets_api.get_markets(
                series_ticker=series_ticker,
                status=status,
                limit=limit,
                cursor=cursor
            )
            
            if not response.markets:
                break
            
            # Filter by volume and convert to MarketInfo
            for market in response.markets:
                if market.volume_24h >= min_volume:
                    all_markets.append(self._market_to_info(market))
            
            # Check for more pages
            cursor = response.cursor
            if not cursor or len(response.markets) < limit:
                break
        
        return all_markets
    
    def get_market(self, ticker: str) -> Optional[MarketInfo]:
        """
        Fetch a single market by ticker.
        
        Args:
            ticker: Market ticker (e.g., "KXFEDDECISION-26JAN-H0")
            
        Returns:
            MarketInfo or None if not found
        """
        try:
            response = self._markets_api.get_market(ticker)
            return self._market_to_info(response.market)
        except Exception:
            return None
    
    def _market_to_info(self, market: Market) -> MarketInfo:
        """Convert SDK Market object to our MarketInfo dataclass."""
        # Extract series ticker from market ticker (e.g., "KXFEDDECISION-26JAN" -> "KXFEDDECISION")
        series_ticker = market.ticker.split("-")[0] if "-" in market.ticker else market.ticker
        
        # Get event_ticker from the SDK object
        event_ticker = getattr(market, 'event_ticker', None)
        
        return MarketInfo(
            ticker=market.ticker,
            series_ticker=series_ticker,
            title=market.title,
            status=market.status,
            volume_24h=market.volume_24h,
            event_ticker=event_ticker,
            open_interest=getattr(market, 'open_interest', None),
            expiration_time=getattr(market, 'expiration_time', None),
        )
    
    def get_markets_by_event(
        self,
        event_ticker: str,
        min_volume: int = 0,
        status: str = "open"
    ) -> List[MarketInfo]:
        """
        Fetch all markets for a specific event.
        
        An event groups related markets (e.g., all Fed Chair nomination candidates).
        
        Args:
            event_ticker: Event ticker (e.g., "KXFEDCHAIRNOM-29")
            min_volume: Minimum 24h volume filter (default: 0 = no filter)
            status: Market status filter (default: "open")
            
        Returns:
            List of MarketInfo objects for all markets in the event
        """
        all_markets = []
        cursor = None
        
        while True:
            response = self._markets_api.get_markets(
                event_ticker=event_ticker,
                status=status,
                limit=200,
                cursor=cursor
            )
            
            if not response.markets:
                break
            
            for market in response.markets:
                if market.volume_24h >= min_volume:
                    all_markets.append(self._market_to_info(market))
            
            cursor = response.cursor
            if not cursor or len(response.markets) < 200:
                break
        
        return all_markets
    
    # =========================================================================
    # ORDERBOOK OPERATIONS
    # =========================================================================

    def get_orderbook(self, ticker: str) -> Optional[OrderbookRaw]:
        """
        Fetch raw orderbook data for a market.
        
        Args:
            ticker: Market ticker
            
        Returns:
            OrderbookRaw with yes/no bid arrays, or None on error
            
        Note:
            Kalshi only returns BIDS (not asks) because in a binary market:
            - Yes Ask = 100 - Best No Bid
            - No Ask = 100 - Best Yes Bid
            
            We use raw HTTP requests because the SDK has a bug where it expects
            'true'/'false' keys but the API returns 'yes'/'no'.
        """
        try:
            # Use raw HTTP request to bypass SDK parsing bug
            url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}/orderbook"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            orderbook = data.get("orderbook", {})
            
            # API returns 'yes' and 'no' arrays with [price, quantity] pairs
            yes_bids = orderbook.get("yes") or []
            no_bids = orderbook.get("no") or []
            
            return OrderbookRaw(
                yes_bids=yes_bids,
                no_bids=no_bids,
                ticker=ticker,
                timestamp=datetime.now()
            )
        except requests.RequestException as e:
            print(f"Warning: HTTP error fetching orderbook for {ticker}: {e}")
            return None
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to parse orderbook for {ticker}: {e}")
            return None
    
    def get_orderbook_with_pricing(
        self, 
        ticker: str
    ) -> Tuple[Optional[OrderbookRaw], Optional[MarketPricing]]:
        """
        Fetch orderbook and extract pricing in one call.
        
        Convenience method that combines get_orderbook() with price extraction.
        
        Args:
            ticker: Market ticker
            
        Returns:
            Tuple of (OrderbookRaw, MarketPricing) or (None, None) on error
        """
        raw = self.get_orderbook(ticker)
        if raw is None:
            return None, None
        
        pricing = self._extract_pricing(raw)
        return raw, pricing
    
    def _extract_pricing(self, raw: OrderbookRaw) -> Optional[MarketPricing]:
        """
        Extract best bid/ask prices from raw orderbook.
        
        Applies the Implied Ask rule for binary markets.
        """
        # Need both sides to have bids
        if not raw.yes_bids or not raw.no_bids:
            return None
        
        # Bids are sorted low→high, so best bid is LAST element
        best_yes_bid = float(raw.yes_bids[-1][0])
        best_no_bid = float(raw.no_bids[-1][0])
        
        # Calculate total depth (sum of all quantities)
        yes_depth = sum(level[1] for level in raw.yes_bids)
        no_depth = sum(level[1] for level in raw.no_bids)
        
        pricing = MarketPricing(
            best_yes_bid=best_yes_bid,
            best_no_bid=best_no_bid,
            yes_bid_depth=yes_depth,
            no_bid_depth=no_depth,
        )
        
        # Apply implied ask rule and calculate spreads
        pricing.calculate_implied_asks()
        pricing.calculate_spreads()
        
        return pricing


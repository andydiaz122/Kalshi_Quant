"""
Yahoo Finance Adapter

Provides async access to real-world financial data from Yahoo Finance.
Uses direct HTTP requests to avoid yfinance library caching issues.

Key Features:
- Async wrapper around Yahoo Finance API
- Live price fetching with caching
- Historical data for rolling window calculations
- Support for various ticker types (stocks, indices, yields)

Primary Use Case:
- Fetching Treasury yields (^IRX, ^TNX) for Fed rate correlation
- Comparing real-world rate movements with Kalshi prediction markets

Usage:
    from kalshi_qete.src.adapters.yahoo import YahooAdapter
    
    adapter = YahooAdapter()
    
    # Get current 13-week T-Bill yield
    yield_pct = await adapter.get_live_price("^IRX")
    print(f"Current yield: {yield_pct:.2f}%")
    
    # Get historical data for rolling window
    history = await adapter.get_history("^IRX", period="5d", interval="1h")
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import urllib.request
import urllib.parse
import json

logger = logging.getLogger(__name__)


@dataclass
class PriceSnapshot:
    """
    Snapshot of a financial instrument's price.
    
    Attributes:
        ticker: Symbol (e.g., "^IRX")
        price: Current price/yield
        timestamp: When the data was fetched
        change: Absolute change from previous close
        change_pct: Percentage change from previous close
        volume: Trading volume (if applicable)
    """
    ticker: str
    price: float
    timestamp: datetime
    change: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    
    def __str__(self) -> str:
        change_str = f" ({self.change_pct:+.2f}%)" if self.change_pct else ""
        return f"{self.ticker}: {self.price:.4f}{change_str}"


@dataclass
class HistoricalData:
    """
    Historical price data for rolling window analysis.
    
    Attributes:
        ticker: Symbol
        prices: List of prices (most recent last)
        timestamps: Corresponding timestamps
        period: Time period covered
        interval: Data interval (1m, 5m, 1h, etc.)
    """
    ticker: str
    prices: List[float]
    timestamps: List[datetime]
    period: str
    interval: str
    
    @property
    def mean(self) -> float:
        """Mean price over the period."""
        return sum(self.prices) / len(self.prices) if self.prices else 0.0
    
    @property
    def std(self) -> float:
        """Standard deviation of prices."""
        if len(self.prices) < 2:
            return 0.0
        mean = self.mean
        variance = sum((p - mean) ** 2 for p in self.prices) / len(self.prices)
        return variance ** 0.5
    
    @property
    def latest(self) -> float:
        """Most recent price."""
        return self.prices[-1] if self.prices else 0.0
    
    def z_score(self, value: Optional[float] = None) -> float:
        """
        Calculate z-score for a value (or latest price).
        
        Args:
            value: Value to calculate z-score for (default: latest price)
            
        Returns:
            Z-score (how many std devs from mean)
        """
        if value is None:
            value = self.latest
        
        if self.std == 0:
            return 0.0
        
        return (value - self.mean) / self.std
    
    def __str__(self) -> str:
        return (
            f"HistoricalData({self.ticker}): {len(self.prices)} points, "
            f"mean={self.mean:.4f}, std={self.std:.4f}, latest={self.latest:.4f}"
        )


class YahooAdapter:
    """
    Async adapter for Yahoo Finance data using direct HTTP requests.
    
    Bypasses the yfinance library to avoid caching/database issues.
    Uses Yahoo Finance's public chart API directly.
    
    Common Tickers:
    - ^IRX: 13-Week Treasury Bill Yield (short-term rates)
    - ^TNX: 10-Year Treasury Note Yield
    - ^VIX: Volatility Index
    - SPY: S&P 500 ETF
    - GC=F: Gold Futures
    
    Example:
        >>> adapter = YahooAdapter()
        >>> 
        >>> # Get live yield
        >>> yield_pct = await adapter.get_live_price("^IRX")
        >>> print(f"T-Bill yield: {yield_pct:.2f}%")
        >>> 
        >>> # Get historical for z-score
        >>> history = await adapter.get_history("^IRX", period="5d", interval="1h")
        >>> z = history.z_score()
        >>> print(f"Z-Score: {z:.2f}")
    """
    
    # Default ticker for Fed rate correlation
    DEFAULT_TICKER = "^IRX"  # 13-Week T-Bill Yield
    
    # Yahoo Finance API endpoints
    CHART_API_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    QUOTE_API_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
    
    def __init__(self, cache_ttl_seconds: int = 30):
        """
        Initialize the Yahoo Finance adapter.
        
        Args:
            cache_ttl_seconds: How long to cache prices (default: 30s)
        """
        self.cache_ttl = cache_ttl_seconds
        self._cache: Dict[str, tuple] = {}  # ticker -> (PriceSnapshot, timestamp)
        
        logger.info("YahooAdapter initialized (direct HTTP mode)")
    
    def _make_request_sync(self, url: str) -> Dict[str, Any]:
        """
        Make a synchronous HTTP request to Yahoo Finance.
        
        This runs in a thread pool via asyncio.to_thread().
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        }
        
        req = urllib.request.Request(url, headers=headers)
        
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode('utf-8'))
                return data
        except Exception as e:
            logger.error(f"HTTP request failed: {e}")
            raise
    
    def _get_quote_sync(self, ticker: str) -> Optional[PriceSnapshot]:
        """
        Get current quote using Yahoo's chart API (more reliable, no auth needed).
        
        Uses the most recent data point from a short-period chart request.
        """
        try:
            # Use chart API with 1d range to get current price
            encoded_ticker = urllib.parse.quote(ticker)
            url = f"{self.CHART_API_URL.format(symbol=encoded_ticker)}?range=2d&interval=1h"
            
            data = self._make_request_sync(url)
            
            if 'chart' not in data or not data['chart'].get('result'):
                error = data.get('chart', {}).get('error', {})
                logger.warning(f"No chart data for {ticker}: {error}")
                return None
            
            result = data['chart']['result'][0]
            meta = result.get('meta', {})
            
            # Get current price from meta
            price = meta.get('regularMarketPrice')
            prev_close = meta.get('previousClose') or meta.get('chartPreviousClose')
            
            # Fallback: get from most recent close in data
            if price is None:
                indicators = result.get('indicators', {})
                quotes = indicators.get('quote', [{}])[0]
                closes = quotes.get('close', [])
                # Filter out None values and get last valid price
                valid_closes = [c for c in closes if c is not None]
                if valid_closes:
                    price = valid_closes[-1]
            
            if price is None:
                logger.warning(f"Could not get price for {ticker}")
                return None
            
            change = None
            change_pct = None
            if prev_close and prev_close > 0:
                change = price - prev_close
                change_pct = (change / prev_close) * 100
            
            return PriceSnapshot(
                ticker=ticker,
                price=float(price),
                timestamp=datetime.now(),
                change=change,
                change_pct=change_pct,
                volume=meta.get('regularMarketVolume')
            )
            
        except Exception as e:
            logger.error(f"Error fetching quote for {ticker}: {e}")
            return None
    
    def _get_chart_sync(
        self, 
        ticker: str, 
        period: str = "5d",
        interval: str = "1h"
    ) -> Optional[HistoricalData]:
        """
        Get historical data using Yahoo's chart API.
        
        Args:
            ticker: Symbol to fetch
            period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        """
        try:
            # Build URL with parameters
            encoded_ticker = urllib.parse.quote(ticker)
            url = f"{self.CHART_API_URL.format(symbol=encoded_ticker)}?range={period}&interval={interval}"
            
            data = self._make_request_sync(url)
            
            if 'chart' not in data or not data['chart'].get('result'):
                error = data.get('chart', {}).get('error', {})
                logger.warning(f"No chart data for {ticker}: {error}")
                return None
            
            result = data['chart']['result'][0]
            
            # Extract timestamps and prices
            timestamps_unix = result.get('timestamp', [])
            indicators = result.get('indicators', {})
            quotes = indicators.get('quote', [{}])[0]
            closes = quotes.get('close', [])
            
            if not timestamps_unix or not closes:
                logger.warning(f"Empty chart data for {ticker}")
                return None
            
            # Convert to our format, filtering out None values
            prices = []
            timestamps = []
            for ts, price in zip(timestamps_unix, closes):
                if price is not None:
                    prices.append(float(price))
                    timestamps.append(datetime.fromtimestamp(ts))
            
            if not prices:
                logger.warning(f"No valid prices for {ticker}")
                return None
            
            return HistoricalData(
                ticker=ticker,
                prices=prices,
                timestamps=timestamps,
                period=period,
                interval=interval
            )
            
        except Exception as e:
            logger.error(f"Error fetching chart for {ticker}: {e}")
            return None
    
    async def get_live_price(self, ticker: str = None) -> float:
        """
        Get the current live price/yield for a ticker.
        
        This is the main method for fetching real-time data.
        Uses asyncio.to_thread() to avoid blocking.
        
        Args:
            ticker: Symbol to fetch (default: ^IRX)
            
        Returns:
            Current price/yield as float
            
        Raises:
            ValueError: If price cannot be fetched
        """
        ticker = ticker or self.DEFAULT_TICKER
        
        # Check cache
        if ticker in self._cache:
            snapshot, cached_at = self._cache[ticker]
            if (datetime.now() - cached_at).total_seconds() < self.cache_ttl:
                logger.debug(f"Cache hit for {ticker}")
                return snapshot.price
        
        # Fetch asynchronously (wraps sync call in thread)
        logger.debug(f"Fetching live price for {ticker}")
        snapshot = await asyncio.to_thread(self._get_quote_sync, ticker)
        
        if snapshot is None:
            raise ValueError(f"Could not fetch price for {ticker}")
        
        # Update cache
        self._cache[ticker] = (snapshot, datetime.now())
        
        logger.info(f"Live price: {snapshot}")
        return snapshot.price
    
    async def get_snapshot(self, ticker: str = None) -> PriceSnapshot:
        """
        Get a full price snapshot with change data.
        
        Args:
            ticker: Symbol to fetch (default: ^IRX)
            
        Returns:
            PriceSnapshot with price, change, etc.
        """
        ticker = ticker or self.DEFAULT_TICKER
        
        # Check cache
        if ticker in self._cache:
            snapshot, cached_at = self._cache[ticker]
            if (datetime.now() - cached_at).total_seconds() < self.cache_ttl:
                return snapshot
        
        snapshot = await asyncio.to_thread(self._get_quote_sync, ticker)
        
        if snapshot is None:
            raise ValueError(f"Could not fetch snapshot for {ticker}")
        
        self._cache[ticker] = (snapshot, datetime.now())
        return snapshot
    
    async def get_history(
        self,
        ticker: str = None,
        period: str = "5d",
        interval: str = "1h"
    ) -> HistoricalData:
        """
        Get historical price data for rolling window analysis.
        
        Args:
            ticker: Symbol to fetch (default: ^IRX)
            period: Time period (1d, 5d, 1mo, 3mo, 1y)
            interval: Data interval (1m, 5m, 15m, 30m, 1h, 1d)
            
        Returns:
            HistoricalData with prices and statistics
            
        Note:
            For intraday intervals (1m, 5m, etc.), period must be ≤ 60 days.
            1-minute data is only available for the last 7 days.
        """
        ticker = ticker or self.DEFAULT_TICKER
        
        logger.debug(f"Fetching history for {ticker} (period={period}, interval={interval})")
        
        history = await asyncio.to_thread(
            self._get_chart_sync, 
            ticker, 
            period, 
            interval
        )
        
        if history is None:
            raise ValueError(f"Could not fetch history for {ticker}")
        
        logger.info(f"History: {history}")
        return history
    
    async def get_z_score(
        self,
        ticker: str = None,
        period: str = "5d",
        interval: str = "1h"
    ) -> float:
        """
        Get the current z-score for a ticker.
        
        This is a convenience method that:
        1. Fetches historical data
        2. Calculates mean and std dev
        3. Returns z-score of latest value
        
        Args:
            ticker: Symbol to fetch
            period: Time period for rolling window
            interval: Data interval
            
        Returns:
            Z-score (how many std devs from mean)
        """
        history = await self.get_history(ticker, period, interval)
        return history.z_score()
    
    def clear_cache(self):
        """Clear the price cache."""
        self._cache.clear()
        logger.info("Cache cleared")


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

async def get_treasury_yield(adapter: YahooAdapter = None) -> float:
    """
    Quick function to get the 13-week T-Bill yield.
    
    Returns:
        Current yield as percentage (e.g., 4.5 for 4.5%)
    """
    if adapter is None:
        adapter = YahooAdapter()
    
    return await adapter.get_live_price("^IRX")


async def get_treasury_z_score(
    adapter: YahooAdapter = None,
    period: str = "5d"
) -> float:
    """
    Quick function to get z-score of treasury yield.
    
    Args:
        adapter: YahooAdapter instance
        period: Rolling window period
        
    Returns:
        Z-score of current yield vs recent history
    """
    if adapter is None:
        adapter = YahooAdapter()
    
    return await adapter.get_z_score("^IRX", period=period)

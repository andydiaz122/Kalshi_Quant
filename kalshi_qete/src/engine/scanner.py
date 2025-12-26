"""
Market Scanner

High-level market discovery and filtering utilities.

This module provides methods to:
1. Discover markets by series, event, or criteria
2. Filter by volume, liquidity, spread
3. Get temporally-sorted markets (next N events)
4. Batch fetch orderbook data for multiple markets

The scanner builds on the KalshiAdapter and integrates with
the orderbook parser for comprehensive market analysis.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Callable

from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter, OrderbookRaw
from kalshi_qete.src.db.models import MarketInfo, MarketPricing, OrderbookSnapshot
from kalshi_qete.src.utils.orderbook import extract_best_prices, analyze_orderbook


@dataclass
class MarketWithOrderbook:
    """
    Market info combined with orderbook data.
    
    Convenience class for passing around complete market state.
    """
    market: MarketInfo
    orderbook: Optional[OrderbookRaw]
    pricing: Optional[MarketPricing]
    analysis: Optional[dict]


class MarketScanner:
    """
    High-level market discovery and analysis.
    
    Provides methods to find markets meeting specific criteria
    and fetch their orderbook data in batch.
    
    Example:
        >>> scanner = MarketScanner(adapter)
        >>> markets = scanner.scan_event("KXFEDCHAIRNOM-29", min_volume=1000)
        >>> for m in markets:
        ...     print(f"{m.market.ticker}: {m.pricing.best_yes_bid}¢")
    """
    
    def __init__(self, adapter: KalshiAdapter):
        """
        Initialize scanner with API adapter.
        
        Args:
            adapter: Authenticated KalshiAdapter instance
        """
        self.adapter = adapter
    
    # =========================================================================
    # DISCOVERY METHODS
    # =========================================================================
    
    def scan_series(
        self,
        series_ticker: str,
        min_volume: int = 0,
        status: str = "open",
        fetch_orderbooks: bool = True
    ) -> List[MarketWithOrderbook]:
        """
        Scan all markets in a series.
        
        Args:
            series_ticker: Series to scan (e.g., "KXFEDDECISION")
            min_volume: Minimum 24h volume filter in cents
            status: Market status filter
            fetch_orderbooks: Whether to fetch orderbook data
            
        Returns:
            List of MarketWithOrderbook objects
        """
        markets = self.adapter.get_markets_by_series(
            series_ticker=series_ticker,
            min_volume=min_volume,
            status=status
        )
        
        return self._enrich_markets(markets, fetch_orderbooks)
    
    def scan_event(
        self,
        event_ticker: str,
        min_volume: int = 0,
        fetch_orderbooks: bool = True
    ) -> List[MarketWithOrderbook]:
        """
        Scan all markets in an event.
        
        An event groups related markets (e.g., all Fed Chair candidates).
        
        Args:
            event_ticker: Event to scan (e.g., "KXFEDCHAIRNOM-29")
            min_volume: Minimum 24h volume filter in cents
            fetch_orderbooks: Whether to fetch orderbook data
            
        Returns:
            List of MarketWithOrderbook objects
        """
        markets = self.adapter.get_markets_by_event(
            event_ticker=event_ticker,
            min_volume=min_volume
        )
        
        return self._enrich_markets(markets, fetch_orderbooks)
    
    def scan_top_volume(
        self,
        n: int = 10,
        min_volume: int = 0,
        fetch_orderbooks: bool = True
    ) -> List[MarketWithOrderbook]:
        """
        Get the top N markets by 24h trading volume.
        
        Args:
            n: Number of markets to return
            min_volume: Minimum volume threshold
            fetch_orderbooks: Whether to fetch orderbook data
            
        Returns:
            List of MarketWithOrderbook objects sorted by volume (desc)
        """
        # Fetch a batch of markets
        all_markets = []
        cursor = None
        
        while len(all_markets) < n * 2:  # Fetch extra to ensure we have enough after filtering
            response = self.adapter._markets_api.get_markets(
                status="open",
                limit=200,
                cursor=cursor
            )
            
            if not response.markets:
                break
            
            for m in response.markets:
                if m.volume_24h >= min_volume:
                    all_markets.append(self.adapter._market_to_info(m))
            
            cursor = response.cursor
            if not cursor or len(response.markets) < 200:
                break
        
        # Sort by volume and take top N
        all_markets.sort(key=lambda m: m.volume_24h, reverse=True)
        top_markets = all_markets[:n]
        
        return self._enrich_markets(top_markets, fetch_orderbooks)
    
    # =========================================================================
    # FILTERING METHODS
    # =========================================================================
    
    def filter_by_spread(
        self,
        markets: List[MarketWithOrderbook],
        max_spread: float = 5.0
    ) -> List[MarketWithOrderbook]:
        """
        Filter markets by maximum spread.
        
        Args:
            markets: List of MarketWithOrderbook to filter
            max_spread: Maximum allowed spread in cents
            
        Returns:
            Markets with spread <= max_spread
        """
        return [
            m for m in markets
            if m.pricing and m.pricing.yes_spread is not None
            and m.pricing.yes_spread <= max_spread
        ]
    
    def filter_by_liquidity(
        self,
        markets: List[MarketWithOrderbook],
        min_depth: int = 1000
    ) -> List[MarketWithOrderbook]:
        """
        Filter markets by minimum orderbook depth.
        
        Args:
            markets: List of MarketWithOrderbook to filter
            min_depth: Minimum total depth (yes + no) in contracts
            
        Returns:
            Markets with sufficient liquidity
        """
        return [
            m for m in markets
            if m.pricing and (m.pricing.yes_bid_depth + m.pricing.no_bid_depth) >= min_depth
        ]
    
    def filter_by_two_sided(
        self,
        markets: List[MarketWithOrderbook]
    ) -> List[MarketWithOrderbook]:
        """
        Filter to only markets with bids on BOTH sides.
        
        These markets have active two-sided interest.
        
        Args:
            markets: List of MarketWithOrderbook to filter
            
        Returns:
            Markets with both YES and NO bids
        """
        return [
            m for m in markets
            if m.orderbook and m.orderbook.yes_bids and m.orderbook.no_bids
        ]
    
    def filter_custom(
        self,
        markets: List[MarketWithOrderbook],
        predicate: Callable[[MarketWithOrderbook], bool]
    ) -> List[MarketWithOrderbook]:
        """
        Filter markets using a custom predicate function.
        
        Args:
            markets: List of MarketWithOrderbook to filter
            predicate: Function that returns True for markets to keep
            
        Returns:
            Markets passing the predicate
            
        Example:
            >>> # Keep only markets with yes bid > 30¢
            >>> scanner.filter_custom(markets, lambda m: m.pricing and m.pricing.best_yes_bid > 30)
        """
        return [m for m in markets if predicate(m)]
    
    # =========================================================================
    # SORTING METHODS
    # =========================================================================
    
    def sort_by_date(
        self,
        markets: List[MarketWithOrderbook],
        ascending: bool = True
    ) -> List[MarketWithOrderbook]:
        """
        Sort markets by date extracted from ticker.
        
        Kalshi tickers often contain dates (e.g., "KXFEDDECISION-28JAN-H0").
        This extracts and sorts by those dates.
        
        Args:
            markets: Markets to sort
            ascending: If True, earliest first; if False, latest first
            
        Returns:
            Sorted list of markets
        """
        def extract_date(ticker: str) -> datetime:
            """Extract date from ticker, return far future if not found."""
            # Pattern: -DDMON- (e.g., -28JAN-, -25DEC-)
            match = re.search(r'-(\d{1,2})([A-Z]{3})-', ticker.upper())
            if match:
                day = int(match.group(1))
                month_str = match.group(2)
                months = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
                    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
                    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                month = months.get(month_str, 1)
                year = datetime.now().year
                # If month is in the past, assume next year
                if month < datetime.now().month:
                    year += 1
                try:
                    return datetime(year, month, day)
                except ValueError:
                    pass
            return datetime(9999, 12, 31)  # Far future for non-dated
        
        return sorted(
            markets,
            key=lambda m: extract_date(m.market.ticker),
            reverse=not ascending
        )
    
    def sort_by_volume(
        self,
        markets: List[MarketWithOrderbook],
        ascending: bool = False
    ) -> List[MarketWithOrderbook]:
        """
        Sort markets by 24h trading volume.
        
        Args:
            markets: Markets to sort
            ascending: If True, lowest first; if False, highest first
            
        Returns:
            Sorted list of markets
        """
        return sorted(
            markets,
            key=lambda m: m.market.volume_24h,
            reverse=not ascending
        )
    
    def sort_by_spread(
        self,
        markets: List[MarketWithOrderbook],
        ascending: bool = True
    ) -> List[MarketWithOrderbook]:
        """
        Sort markets by spread (tightest first by default).
        
        Args:
            markets: Markets to sort
            ascending: If True, tightest spread first
            
        Returns:
            Sorted list of markets
        """
        def get_spread(m: MarketWithOrderbook) -> float:
            if m.pricing and m.pricing.yes_spread is not None:
                return m.pricing.yes_spread
            return float('inf')  # No spread = worst
        
        return sorted(markets, key=get_spread, reverse=not ascending)
    
    # =========================================================================
    # SNAPSHOT CREATION
    # =========================================================================
    
    def create_snapshots(
        self,
        markets: List[MarketWithOrderbook]
    ) -> List[OrderbookSnapshot]:
        """
        Convert MarketWithOrderbook list to OrderbookSnapshot list.
        
        Creates database-ready snapshot records from scanned markets.
        
        Args:
            markets: List of MarketWithOrderbook objects
            
        Returns:
            List of OrderbookSnapshot objects ready for DB insertion
        """
        snapshots = []
        timestamp = datetime.now()
        
        for m in markets:
            if m.pricing is None:
                continue
            
            snapshot = OrderbookSnapshot(
                snapshot_ts=timestamp,
                ticker=m.market.ticker,
                series_ticker=m.market.series_ticker,
                market_title=m.market.title,
                best_yes_bid=m.pricing.best_yes_bid,
                best_yes_ask=m.pricing.best_yes_ask,
                best_no_bid=m.pricing.best_no_bid,
                best_no_ask=m.pricing.best_no_ask,
                yes_spread=m.pricing.yes_spread,
                no_spread=m.pricing.no_spread,
                volume_24h=m.market.volume_24h,
                yes_bid_depth=m.pricing.yes_bid_depth,
                no_bid_depth=m.pricing.no_bid_depth,
            )
            snapshots.append(snapshot)
        
        return snapshots
    
    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================
    
    def _enrich_markets(
        self,
        markets: List[MarketInfo],
        fetch_orderbooks: bool
    ) -> List[MarketWithOrderbook]:
        """
        Enrich market info with orderbook data and analysis.
        
        Args:
            markets: Raw market info list
            fetch_orderbooks: Whether to fetch orderbook data
            
        Returns:
            List of MarketWithOrderbook with full data
        """
        results = []
        
        for market in markets:
            orderbook = None
            pricing = None
            analysis = None
            
            if fetch_orderbooks:
                orderbook = self.adapter.get_orderbook(market.ticker)
                
                if orderbook:
                    pricing = extract_best_prices(orderbook.yes_bids, orderbook.no_bids)
                    analysis = analyze_orderbook(orderbook.yes_bids, orderbook.no_bids)
            
            results.append(MarketWithOrderbook(
                market=market,
                orderbook=orderbook,
                pricing=pricing,
                analysis=analysis
            ))
        
        return results
    
    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================
    
    def get_liquid_markets(
        self,
        series_ticker: str,
        min_volume: int = 10000,
        max_spread: float = 5.0,
        require_two_sided: bool = True
    ) -> List[MarketWithOrderbook]:
        """
        Get liquid, tradeable markets in a series.
        
        Convenience method combining common filters.
        
        Args:
            series_ticker: Series to scan
            min_volume: Minimum 24h volume
            max_spread: Maximum allowed spread
            require_two_sided: Require bids on both sides
            
        Returns:
            Filtered list of liquid markets
        """
        markets = self.scan_series(series_ticker, min_volume=min_volume)
        
        if require_two_sided:
            markets = self.filter_by_two_sided(markets)
        
        markets = self.filter_by_spread(markets, max_spread=max_spread)
        
        return self.sort_by_volume(markets)
    
    def summarize_event(self, event_ticker: str) -> Dict:
        """
        Get a summary of an event's markets.
        
        Args:
            event_ticker: Event to summarize
            
        Returns:
            Dictionary with event statistics
        """
        markets = self.scan_event(event_ticker)
        
        total_markets = len(markets)
        markets_with_orderbook = len([m for m in markets if m.orderbook and (m.orderbook.yes_bids or m.orderbook.no_bids)])
        two_sided = len([m for m in markets if m.orderbook and m.orderbook.yes_bids and m.orderbook.no_bids])
        
        total_volume = sum(m.market.volume_24h for m in markets)
        
        # Find market with highest yes bid (most likely outcome)
        leader = None
        leader_price = 0
        for m in markets:
            if m.pricing and m.pricing.best_yes_bid > leader_price:
                leader = m.market
                leader_price = m.pricing.best_yes_bid
        
        return {
            "event_ticker": event_ticker,
            "total_markets": total_markets,
            "markets_with_orderbook": markets_with_orderbook,
            "two_sided_markets": two_sided,
            "total_volume_24h": total_volume,
            "leader_ticker": leader.ticker if leader else None,
            "leader_title": leader.title if leader else None,
            "leader_yes_bid": leader_price,
        }


"""
Market scanner for discovering and retrieving orderbook data from Kalshi API.
"""
import json
from datetime import datetime
from typing import List, Optional
from pydantic import ValidationError
from kalshi_python_sync import KalshiClient
from kalshi_python_sync.exceptions import ApiException, NotFoundException
from kalshi_python_sync.models.market import Market

from ingestion.orderbook_parser import extract_orderbook_prices
from ingestion.market_date_parser import sort_markets_by_date
from models.market_data import OrderbookSnapshot
from database.db_manager import DatabaseManager


class MarketScanner:
    """Scans Kalshi markets and retrieves orderbook data."""
    
    def __init__(self, client: KalshiClient):
        """
        Initialize market scanner with authenticated Kalshi client.
        
        Args:
            client: Authenticated KalshiClient instance
        """
        self.client = client
    
    def scan_series_markets(
        self, 
        series_ticker: str, 
        min_volume: int = 100000,
        status: str = "open"
    ) -> List[Market]:
        """
        Fetch all markets in a series with volume filtering.
        Handles pagination to retrieve all results.
        
        Args:
            series_ticker: Series ticker to filter by (e.g., "KXFEDDECISION")
            min_volume: Minimum 24-hour volume in cents (default: 100000 = $1000)
            status: Market status filter (default: "open")
            
        Returns:
            List of Market objects that meet the volume criteria
        """
        all_markets = []
        cursor = None
        
        while True:
            # Fetch markets with pagination
            try:
                if cursor:
                    markets_response = self.client.get_markets(
                        series_ticker=series_ticker,
                        status=status,
                        limit=1000,  # Max per page
                        cursor=cursor
                    )
                else:
                    markets_response = self.client.get_markets(
                        series_ticker=series_ticker,
                        status=status,
                        limit=1000
                    )
            except ApiException as e:
                print(f"⚠️  API error fetching markets: {e}")
                break
            
            if not markets_response.markets:
                break
            
            # Filter by volume
            filtered_markets = [
                m for m in markets_response.markets
                if m.volume_24h >= min_volume
            ]
            all_markets.extend(filtered_markets)
            
            # Check if there are more pages
            cursor = markets_response.cursor
            if not cursor or len(markets_response.markets) < 1000:
                break
        
        return all_markets
    
    def get_next_n_meetings(
        self,
        series_ticker: str,
        n: int = 4,
        min_volume: int = 100000,
        status: str = "open"
    ) -> List[Market]:
        """
        Get the next N Fed meetings sorted by date (earliest first).
        
        Fetches all markets in the series, filters by volume, sorts by meeting date,
        and returns the first N markets (next upcoming meetings).
        
        Args:
            series_ticker: Series ticker to filter by (e.g., "KXFEDDECISION")
            n: Number of meetings to return (default: 4)
            min_volume: Minimum 24-hour volume in cents (default: 100000 = $1000)
            status: Market status filter (default: "open")
            
        Returns:
            List of Market objects representing the next N Fed meetings, sorted by date
            
        Examples:
            >>> scanner.get_next_n_meetings("KXFEDDECISION", n=4)
            [Market(ticker="KXFEDDECISION-26JAN-H0"), ...]  # Next 4 meetings
        """
        # Fetch all markets meeting volume criteria
        all_markets = self.scan_series_markets(
            series_ticker=series_ticker,
            min_volume=min_volume,
            status=status
        )
        
        if not all_markets:
            return []
        
        # Sort markets by meeting date (earliest first)
        sorted_markets = sort_markets_by_date(all_markets)
        
        # Return first N markets (next upcoming meetings)
        return sorted_markets[:n]
    
    def get_market_metadata(self, ticker: str) -> Optional[Market]:
        """
        Get full market object for metadata.
        
        Args:
            ticker: Market ticker
            
        Returns:
            Market object or None if not found
        """
        try:
            market_response = self.client.get_market(ticker)
            return market_response.market
        except (NotFoundException, ApiException):
            return None
    
    def get_orderbook_snapshot(
        self, 
        ticker: str, 
        market: Optional[Market] = None,
        series_ticker: Optional[str] = None
    ) -> Optional[OrderbookSnapshot]:
        """
        Retrieve orderbook snapshot for a market.
        Preserves raw HTTP fallback logic for Pydantic validation errors.
        
        Args:
            ticker: Market ticker
            market: Optional Market object for metadata (volume_24h, title, etc.)
            series_ticker: Optional series ticker (e.g., "KXFEDDECISION"). If not provided, extracted from ticker.
            
        Returns:
            OrderbookSnapshot object or None if insufficient data
        """
        yes_bids = None
        no_bids = None
        yes_asks = None
        no_asks = None
        
        # Get market metadata if not provided
        if market is None:
            market = self.get_market_metadata(ticker)
        
        # Try to get market data which may have ask prices
        market_data = market
        
        try:
            orderbook_response = self.client.get_market_orderbook(ticker)
            orderbook = orderbook_response.orderbook
            
            # Extract Yes and No bids
            # Note: orderbook uses 'var_true' (aliased as "true") for Yes bids
            # and 'var_false' (aliased as "false") for No bids
            yes_bids = orderbook.var_true  # Yes bids: [[price, quantity], ...]
            no_bids = orderbook.var_false  # No bids: [[price, quantity], ...]
        except ValidationError:
            # Handle Pydantic validation error - API returns integers where strings expected
            # for yes_dollars/no_dollars, but we only need var_true/var_false
            # Make a direct HTTP request to bypass validation
            # Construct the full URL - resource path is /markets/{ticker}/orderbook
            # and base path already includes /trade-api/v2
            resource_path = f"/markets/{ticker}/orderbook"
            full_url = f"{self.client.configuration._base_path}{resource_path}"
            
            # Use client's call_api method which handles URL construction and auth automatically
            # Note: call_api will add Kalshi auth headers using the url parameter
            response = self.client.call_api(
                method="GET",
                url=full_url,
                header_params={}
            )
            
            # Read the response data (required for RESTResponse objects)
            response_data = response.read()
            
            # Check response status
            if response.status != 200:
                raise ApiException(
                    http_resp=response,
                    body=response_data.decode('utf-8') if isinstance(response_data, bytes) else str(response_data)
                )
            
            # Decode response data - handle both bytes and already-decoded strings
            if isinstance(response_data, bytes):
                response_text = response_data.decode('utf-8')
            elif isinstance(response_data, str):
                response_text = response_data
            else:
                response_text = str(response_data)
            
            # Strip any whitespace that might cause JSON parsing issues
            response_text = response_text.strip()
            
            # Parse raw JSON response
            try:
                raw_json = json.loads(response_text)
            except json.JSONDecodeError as e:
                print(f"   ⚠️  JSON decode error. Response preview: {response_text[:200]}")
                raise
            
            orderbook_data = raw_json.get('orderbook', {})
            
            # Raw JSON uses 'yes' and 'no', not 'true' and 'false'
            yes_bids = orderbook_data.get('yes')  # Raw JSON uses "yes"
            no_bids = orderbook_data.get('no')  # Raw JSON uses "no"
            
            # Check if there are ask fields (maybe 'yes_asks' or similar)
            # Also check the full raw JSON for any ask-related fields
            yes_asks = (orderbook_data.get('yes_asks') or 
                       orderbook_data.get('yes_ask') or 
                       orderbook_data.get('asks_yes') or
                       raw_json.get('yes_asks') or
                       raw_json.get('yes_ask'))
            no_asks = (orderbook_data.get('no_asks') or 
                      orderbook_data.get('no_ask') or 
                      orderbook_data.get('asks_no') or
                      raw_json.get('no_asks') or
                      raw_json.get('no_ask'))
        
        # Use market data for asks if orderbook doesn't have them
        if not yes_asks and market_data and hasattr(market_data, 'yes_ask'):
            yes_asks = [[market_data.yes_ask, 0]]  # Convert to list format
        if not no_asks and market_data and hasattr(market_data, 'no_ask'):
            no_asks = [[market_data.no_ask, 0]]  # Convert to list format
        
        # Extract actual bid and ask prices
        pricing = extract_orderbook_prices(yes_bids, no_bids, yes_asks, no_asks)
        
        if pricing is None:
            return None
        
        # Extract series ticker from market ticker if not provided (e.g., "KXFEDDECISION-26JAN" -> "KXFEDDECISION")
        if series_ticker is None and ticker and '-' in ticker:
            series_ticker = ticker.split('-')[0]
        
        # Create OrderbookSnapshot
        snapshot = OrderbookSnapshot(
            snapshot_timestamp=datetime.now(),
            ticker=ticker,
            market_title=market_data.title if market_data else None,
            series_ticker=series_ticker,
            best_yes_bid=pricing.best_yes_bid,
            best_yes_ask=pricing.best_yes_ask,
            best_no_bid=pricing.best_no_bid,
            best_no_ask=pricing.best_no_ask,
            yes_spread=pricing.yes_spread,
            no_spread=pricing.no_spread,
            volume_24h=market_data.volume_24h if market_data else None
        )
        
        return snapshot
    
    def scan_and_store_markets(
        self, 
        series_ticker: str, 
        db_manager: DatabaseManager,
        min_volume: int = 100000
    ) -> None:
        """
        Orchestrate: scan markets → get snapshots → insert to DB.
        Handles errors gracefully (log, continue to next market).
        
        Args:
            series_ticker: Series ticker to scan (e.g., "KXFEDDECISION")
            db_manager: DatabaseManager instance for storing snapshots
            min_volume: Minimum 24-hour volume in cents (default: 100000 = $1000)
        """
        print(f"--- 🔍 SCANNING MARKETS IN SERIES: {series_ticker} ---")
        
        # Scan markets with volume filter
        markets = self.scan_series_markets(series_ticker, min_volume=min_volume)
        
        if not markets:
            print(f"⚠️  No markets found in {series_ticker} with volume >= ${min_volume/100:.2f}")
            return
        
        print(f"✅ Found {len(markets)} markets meeting volume criteria")
        
        # Process each market
        snapshots = []
        success_count = 0
        error_count = 0
        
        for market in markets:
            try:
                print(f"   Processing: {market.ticker} - {market.title}")
                snapshot = self.get_orderbook_snapshot(
                    market.ticker, 
                    market=market,
                    series_ticker=series_ticker
                )
                
                if snapshot:
                    snapshots.append(snapshot)
                    success_count += 1
                    print(f"      ✅ Snapshot created: Yes Bid {snapshot.best_yes_bid:.2f}¢ | No Bid {snapshot.best_no_bid:.2f}¢")
                else:
                    print(f"      ⚠️  Insufficient orderbook data for {market.ticker}")
                    error_count += 1
            except Exception as e:
                print(f"      ❌ Error processing {market.ticker}: {e}")
                error_count += 1
                continue
        
        # Batch insert all snapshots
        if snapshots:
            try:
                db_manager.insert_snapshots_batch(snapshots)
                print(f"\n✅ Successfully stored {len(snapshots)} orderbook snapshots")
            except Exception as e:
                print(f"\n❌ Error storing snapshots: {e}")
        
        print(f"\n--- Summary: {success_count} successful, {error_count} errors ---")


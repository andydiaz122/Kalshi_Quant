#!/usr/bin/env python3
"""
Kalshi Connection and Pricing Script

This script authenticates with the Kalshi API, fetches Federal Funds Rate markets,
retrieves orderbook data, and calculates bid/ask spreads using the "Implied Ask" rule
for binary prediction markets.

The "Implied Ask" Rule:
- Kalshi API only returns Bids for 'Yes' and Bids for 'No'
- The "Ask" for 'Yes' is calculated as: 100 cents - Best_No_Bid
- The "Spread" is: (100 - Best_No_Bid) - Best_Yes_Bid
"""

import os
import sys
from pathlib import Path

# Verify required packages are available (check if we're using venv)
try:
    import pydantic
except ImportError:
    _script_dir = Path(__file__).parent.resolve()
    _venv_python = _script_dir / "venv" / "bin" / "python3"
    print("❌ ERROR: Required packages not found!")
    print(f"   Current Python: {sys.executable}")
    print("\n💡 SOLUTION: Activate the venv first:")
    print("   source venv/bin/activate")
    print("   python3 connect_and_price.py")
    print("\n   OR use the venv's Python directly:")
    if _venv_python.exists():
        print(f"   {_venv_python} connect_and_price.py")
    else:
        print("   ./venv/bin/python3 connect_and_price.py")
    sys.exit(1)

import json
import json
from pathlib import Path
from typing import Optional, Tuple, List
from pydantic import ValidationError
from kalshi_python_sync import KalshiClient, Configuration
from kalshi_python_sync.exceptions import (
    UnauthorizedException,
    ApiException,
    NotFoundException,
)


# ==========================================
# CONFIGURATION CONSTANTS
# ==========================================
KEY_ID = "0ac60c80-d575-480e-979b-aa5050a61c1b"  # Replace with your actual Kalshi API Key ID
KEY_FILE_PATH = Path("My_First_API_Key.key")  # RSA PEM format private key file
MIN_DAILY_VOLUME = 100000  # $1000 in cents - minimum 24-hour volume to filter markets


def load_private_key_pem(key_file_path: Path) -> str:
    """
    Load RSA private key from PEM file and return as string.
    
    Args:
        key_file_path: Path to the RSA private key file in PEM format
        
    Returns:
        Private key content as string
        
    Raises:
        FileNotFoundError: If the key file doesn't exist
        ValueError: If the key file cannot be parsed
    """
    if not key_file_path.exists():
        raise FileNotFoundError(
            f"❌ ERROR: Could not find key file '{key_file_path}' in the current directory.\n"
            f"   -> Please ensure 'kalshi.key' is in the project root directory."
        )
    
    try:
        with open(key_file_path, "r", encoding="utf-8") as key_file:
            private_key_pem = key_file.read()
        return private_key_pem
    except Exception as e:
        raise ValueError(
            f"❌ ERROR: Failed to read key file '{key_file_path}': {e}"
        ) from e


def extract_orderbook_prices(
    yes_bids: Optional[list],
    no_bids: Optional[list],
    yes_asks: Optional[list] = None,
    no_asks: Optional[list] = None
) -> Optional[Tuple[float, float, float, float]]:
    """
    Extract actual bid and ask prices from orderbook data.
    
    Args:
        yes_bids: List of [price, quantity] pairs for Yes bids, or None
        no_bids: List of [price, quantity] pairs for No bids, or None
        yes_asks: List of [price, quantity] pairs for Yes asks, or None
        no_asks: List of [price, quantity] pairs for No asks, or None
        
    Returns:
        Tuple of (best_yes_bid, best_yes_ask, best_no_bid, best_no_ask) in cents, or None if insufficient data
    """
    # Extract best Yes bid (LAST element is highest price per API docs)
    # Orderbook entries are [price, quantity] pairs, sorted from lowest to highest
    # Best bid is the HIGHEST price, which is the LAST element in the array
    best_yes_bid = None
    if yes_bids and len(yes_bids) > 0:
        # Last element is the highest price (best bid)
        best_yes_bid = float(yes_bids[-1][0])  # Last element, first value is price
    
    # Extract best No bid (LAST element is highest price per API docs)
    best_no_bid = None
    if no_bids and len(no_bids) > 0:
        # Last element is the highest price (best bid)
        best_no_bid = float(no_bids[-1][0])  # Last element, first value is price
    
    # Extract best Yes ask (FIRST element is lowest ask price)
    # Asks are typically sorted from lowest to highest, so first is best
    best_yes_ask = None
    if yes_asks and len(yes_asks) > 0:
        # First element is the lowest price (best ask)
        best_yes_ask = float(yes_asks[0][0])  # First element, first value is price
    elif yes_bids and best_no_bid is not None:
        # Fallback to implied ask if no actual asks available
        best_yes_ask = 100.0 - best_no_bid
    
    # Extract best No ask (FIRST element is lowest ask price)
    best_no_ask = None
    if no_asks and len(no_asks) > 0:
        # First element is the lowest price (best ask)
        best_no_ask = float(no_asks[0][0])  # First element, first value is price
    elif no_bids and best_yes_bid is not None:
        # Fallback to implied ask if no actual asks available
        best_no_ask = 100.0 - best_yes_bid
    
    # Need at least bids to return data
    if best_yes_bid is None or best_no_bid is None:
        return None
    
    return (best_yes_bid, best_yes_ask, best_no_bid, best_no_ask)


def scan_series_markets(client: KalshiClient, series_ticker: str, min_volume: int = 100000) -> List:
    """
    Scan all markets in a series with volume filtering and pagination support.
    
    Args:
        client: Authenticated KalshiClient instance
        series_ticker: Series ticker to filter by (e.g., "KXFEDDECISION")
        min_volume: Minimum 24-hour volume in cents (default: 100000 = $1000)
        
    Returns:
        List of Market objects that meet the volume criteria
    """
    all_markets = []
    cursor = None
    
    print(f"--- 🔍 SCANNING MARKETS IN SERIES: {series_ticker} (MIN VOLUME: ${min_volume/100:.2f}) ---")
    
    while True:
        try:
            if cursor:
                markets_response = client.get_markets(
                    series_ticker=series_ticker,
                    status="open",
                    limit=1000,  # Max per page
                    cursor=cursor
                )
            else:
                markets_response = client.get_markets(
                    series_ticker=series_ticker,
                    status="open",
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
        
        print(f"   Found {len(filtered_markets)} markets meeting volume criteria (page total: {len(markets_response.markets)})")
        
        # Check if there are more pages
        cursor = markets_response.cursor
        if not cursor or len(markets_response.markets) < 1000:
            break
    
    print(f"✅ Total qualifying markets: {len(all_markets)}")
    return all_markets


def process_market_orderbook(client: KalshiClient, ticker: str, market_data=None):
    """
    Retrieve and process orderbook data for a single market.
    Preserves the existing orderbook retrieval logic including raw HTTP fallback.
    
    Args:
        client: Authenticated KalshiClient instance
        ticker: Market ticker
        market_data: Optional Market object (if already fetched)
        
    Returns:
        Tuple of (price_data, market_data) or (None, None) if insufficient data
    """
    yes_bids = None
    no_bids = None
    yes_asks = None
    no_asks = None
    
    # Get market data if not provided
    if market_data is None:
        try:
            market_response = client.get_market(ticker)
            market_data = market_response.market
        except Exception as e:
            print(f"   ⚠️  Could not retrieve market data: {e}")
            market_data = None
    
    try:
        orderbook_response = client.get_market_orderbook(ticker)
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
        full_url = f"{client.configuration._base_path}{resource_path}"
        
        # Use client's call_api method which handles URL construction and auth automatically
        # Note: call_api will add Kalshi auth headers using the url parameter
        response = client.call_api(
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
            # Debug: print first 200 chars if JSON parsing fails
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
    price_data = extract_orderbook_prices(yes_bids, no_bids, yes_asks, no_asks)
    
    return price_data, market_data


def main():
    """
    Main execution function: authenticate, fetch market, get orderbook, calculate spread.
    """
    # Validate key ID is set
    if KEY_ID == "YOUR_KEY_ID_HERE":
        print("❌ ERROR: Please set KEY_ID constant with your actual Kalshi API Key ID")
        return
    
    try:
        # Load private key
        print("--- 🔌 INITIALIZING KALSHI CONNECTION ---")
        private_key_pem = load_private_key_pem(KEY_FILE_PATH)
        print(f"✅ Loaded private key from '{KEY_FILE_PATH}'")
        
        # Initialize configuration
        config = Configuration()
        
        # Initialize KalshiClient with authentication
        # KalshiClient expects api_key_id and private_key_pem in config
        # We'll use KalshiAuth directly via the client
        client = KalshiClient(configuration=config)
        
        # Set up Kalshi authentication
        # Note: KalshiAuth expects key_id and private_key_pem (as string)
        from kalshi_python_sync.auth import KalshiAuth
        client.kalshi_auth = KalshiAuth(KEY_ID, private_key_pem)
        print("✅ Authentication configured")
        
        # Scan all markets in KXFEDDECISION series with volume filtering
        print(f"\n--- 🔍 SCANNING ALL MARKETS IN KXFEDDECISION SERIES ---")
        qualifying_markets = scan_series_markets(client, "KXFEDDECISION", MIN_DAILY_VOLUME)
        
        if not qualifying_markets:
            print(f"⚠️  No markets found with volume >= ${MIN_DAILY_VOLUME/100:.2f}")
            return
        
        # Process each qualifying market
        print(f"\n--- 📖 PROCESSING ORDERBOOKS FOR {len(qualifying_markets)} MARKETS ---")
        success_count = 0
        error_count = 0
        
        for market in qualifying_markets:
            ticker = market.ticker
            print(f"\n{'='*60}")
            print(f"MARKET: {ticker}")
            print(f"TITLE: {market.title}")
            print(f"VOLUME (24h): ${market.volume_24h/100:.2f}")
            print(f"{'='*60}")
            
            try:
                # Get orderbook for this market
                price_data, market_data = process_market_orderbook(client, ticker, market)
                
                if price_data is None:
                    print(f"❌ ERROR: Insufficient orderbook data for {ticker}")
                    if market_data:
                        if not hasattr(market_data, 'yes_bid') or market_data.yes_bid is None:
                            print("   -> No Yes bids available")
                        if not hasattr(market_data, 'no_bid') or market_data.no_bid is None:
                            print("   -> No No bids available")
                    error_count += 1
                    continue
                
                best_yes_bid, best_yes_ask, best_no_bid, best_no_ask = price_data
                
                # Calculate spreads
                yes_spread = best_yes_ask - best_yes_bid if best_yes_ask is not None else None
                no_spread = best_no_ask - best_no_bid if best_no_ask is not None else None
                
                # Print formatted output
                print(f"\n--- 💰 MARKET PRICING (ACTUAL ORDERBOOK DATA) ---")
                print(f"   Yes: Bid {best_yes_bid:.2f}¢ | Ask {best_yes_ask:.2f}¢ | Spread {yes_spread:.2f}¢" if best_yes_ask else f"   Yes: Bid {best_yes_bid:.2f}¢ | Ask N/A")
                print(f"   No:  Bid {best_no_bid:.2f}¢ | Ask {best_no_ask:.2f}¢ | Spread {no_spread:.2f}¢" if best_no_ask else f"   No:  Bid {best_no_bid:.2f}¢ | Ask N/A")
                print(f"\nMARKET: {ticker}")
                print(f"  YES: BID {best_yes_bid:.2f}¢ | ASK {best_yes_ask:.2f}¢ | SPREAD {yes_spread:.2f}¢" if best_yes_ask else f"  YES: BID {best_yes_bid:.2f}¢ | ASK N/A")
                print(f"  NO:  BID {best_no_bid:.2f}¢ | ASK {best_no_ask:.2f}¢ | SPREAD {no_spread:.2f}¢" if best_no_ask else f"  NO:  BID {best_no_bid:.2f}¢ | ASK N/A")
                
                success_count += 1
                
            except Exception as e:
                print(f"❌ ERROR processing {ticker}: {e}")
                error_count += 1
                continue
        
        # Summary
        print(f"\n{'='*60}")
        print(f"✅ SUCCESS: Processed {success_count} markets successfully")
        if error_count > 0:
            print(f"⚠️  ERRORS: {error_count} markets failed")
        print(f"{'='*60}")
        
    except FileNotFoundError as e:
        print(f"\n{e}")
    except UnauthorizedException as e:
        print(f"\n❌ AUTHENTICATION ERROR: {e}")
        print("   -> Please verify your KEY_ID and private key file are correct")
    except NotFoundException as e:
        print(f"\n❌ NOT FOUND ERROR: {e}")
        print("   -> The requested market or resource was not found")
    except ValidationError as e:
        print(f"\n❌ VALIDATION ERROR: Failed to parse API response")
        print(f"   -> Error details: {e}")
        print("   -> This may indicate an API response format change")
    except ApiException as e:
        print(f"\n❌ API ERROR: {e}")
        print("   -> Check your network connection and API status")
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        print(f"   -> Error type: {type(e).__name__}")


if __name__ == "__main__":
    main()


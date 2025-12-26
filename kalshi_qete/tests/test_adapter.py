#!/usr/bin/env python3
"""
Test script for Kalshi Adapter

Run this to verify the adapter can:
1. Connect to Kalshi API
2. Fetch exchange status
3. Fetch markets in a series
4. Get orderbook data

Usage:
    python -m kalshi_qete.tests.test_adapter
    
    OR from project root:
    python kalshi_qete/tests/test_adapter.py
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter


def test_adapter():
    """Test the Kalshi adapter with real API calls."""
    
    print("=" * 60)
    print("KALSHI ADAPTER TEST")
    print("=" * 60)
    
    # Step 1: Create adapter
    print("\n[1] Creating adapter...")
    try:
        adapter = KalshiAdapter(
            key_id=config.KEY_ID,
            key_file_path=config.KEY_FILE_PATH
        )
        print("    ✓ Adapter created successfully")
    except FileNotFoundError as e:
        print(f"    ✗ Key file not found: {e}")
        return False
    except Exception as e:
        print(f"    ✗ Failed to create adapter: {e}")
        return False
    
    # Step 2: Check exchange status
    print("\n[2] Checking exchange status...")
    try:
        status = adapter.get_exchange_status()
        print(f"    ✓ Exchange active: {status['exchange_active']}")
        print(f"    ✓ Trading active:  {status['trading_active']}")
        print(f"    ✓ Is open for trading: {adapter.is_exchange_open()}")
    except Exception as e:
        print(f"    ✗ Failed to get exchange status: {e}")
        return False
    
    # Step 3: Fetch markets in KXFEDDECISION series
    print("\n[3] Fetching Fed Decision markets...")
    try:
        markets = adapter.get_markets_by_series(
            series_ticker="KXFEDDECISION",
            min_volume=config.MIN_DAILY_VOLUME,
            status="open"
        )
        print(f"    ✓ Found {len(markets)} markets with volume >= ${config.MIN_DAILY_VOLUME/100:.0f}")
        
        if markets:
            # Show first 3 markets
            for i, market in enumerate(markets[:3]):
                print(f"      [{i+1}] {market.ticker}")
                print(f"          Title: {market.title}")
                print(f"          Volume: ${market.volume_24h/100:,.0f}")
    except Exception as e:
        print(f"    ✗ Failed to fetch markets: {e}")
        return False
    
    # Step 4: Get orderbook for first market
    if markets:
        print(f"\n[4] Fetching orderbook for {markets[0].ticker}...")
        try:
            raw, pricing = adapter.get_orderbook_with_pricing(markets[0].ticker)
            
            if raw and pricing:
                print(f"    ✓ Orderbook retrieved")
                print(f"      Yes bids: {len(raw.yes_bids)} levels")
                print(f"      No bids:  {len(raw.no_bids)} levels")
                print(f"\n    Pricing:")
                print(f"      Best Yes Bid: {pricing.best_yes_bid:.2f}¢")
                print(f"      Best Yes Ask: {pricing.best_yes_ask:.2f}¢ (implied)")
                print(f"      Yes Spread:   {pricing.yes_spread:.2f}¢")
                print(f"      Best No Bid:  {pricing.best_no_bid:.2f}¢")
                print(f"      Best No Ask:  {pricing.best_no_ask:.2f}¢ (implied)")
                print(f"      No Spread:    {pricing.no_spread:.2f}¢")
            else:
                print(f"    ⚠ Orderbook empty or insufficient data")
        except Exception as e:
            print(f"    ✗ Failed to fetch orderbook: {e}")
            return False
    
    print("\n" + "=" * 60)
    print("✓ ALL TESTS PASSED")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_adapter()
    sys.exit(0 if success else 1)


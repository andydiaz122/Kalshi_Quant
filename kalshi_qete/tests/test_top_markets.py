#!/usr/bin/env python3
"""
Test script: Fetch top 5 markets by 24h volume and display orderbook data.

This verifies our adapter can:
1. Fetch markets across all series
2. Sort by volume
3. Get and parse orderbook data correctly

Usage:
    PYTHONPATH=/Users/christiandiaz/Kalshi_Quant python kalshi_qete/tests/test_top_markets.py
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter


def get_top_markets_by_volume(adapter: KalshiAdapter, n: int = 5):
    """
    Fetch the top N markets by 24h trading volume.
    
    The API doesn't have a sort parameter, so we:
    1. Fetch a large batch of open markets
    2. Sort client-side by volume
    3. Return top N
    """
    all_markets = []
    cursor = None
    
    # Fetch up to 1000 markets (should be enough to find liquid ones)
    while len(all_markets) < 1000:
        response = adapter._markets_api.get_markets(
            status="open",
            limit=200,
            cursor=cursor
        )
        
        if not response.markets:
            break
            
        all_markets.extend(response.markets)
        cursor = response.cursor
        
        if not cursor or len(response.markets) < 200:
            break
    
    # Sort by 24h volume (descending)
    sorted_markets = sorted(all_markets, key=lambda m: m.volume_24h, reverse=True)
    
    return sorted_markets[:n]


def main():
    print("=" * 70)
    print("TOP 5 KALSHI MARKETS BY 24H VOLUME - ORDERBOOK TEST")
    print("=" * 70)
    
    # Create adapter
    print("\nConnecting to Kalshi API...")
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    print("✓ Connected\n")
    
    # Get top 5 markets
    print("Fetching top 5 markets by 24h volume...")
    top_markets = get_top_markets_by_volume(adapter, n=5)
    
    if not top_markets:
        print("✗ No markets found!")
        return False
    
    print(f"✓ Found {len(top_markets)} markets\n")
    
    # Display each market and its orderbook
    for i, market in enumerate(top_markets, 1):
        print("-" * 70)
        print(f"#{i} {market.ticker}")
        print(f"    Title:  {market.title}")
        print(f"    Volume: ${market.volume_24h / 100:,.2f} (24h)")
        print(f"    Status: {market.status}")
        
        # Get orderbook
        raw = adapter.get_orderbook(market.ticker)
        
        if raw is None:
            print("    Orderbook: ERROR fetching")
            continue
        
        if not raw.yes_bids and not raw.no_bids:
            print("    Orderbook: EMPTY (no bids)")
            continue
        
        print(f"\n    YES BIDS ({len(raw.yes_bids)} levels):")
        if raw.yes_bids:
            # Show top 3 (highest price = best bid = last in array)
            for level in reversed(raw.yes_bids[-3:]):
                price, qty = level
                print(f"      {price:>3}¢  x {qty}")
        else:
            print("      (none)")
        
        print(f"\n    NO BIDS ({len(raw.no_bids)} levels):")
        if raw.no_bids:
            # Show top 3 (highest price = best bid = last in array)
            for level in reversed(raw.no_bids[-3:]):
                price, qty = level
                print(f"      {price:>3}¢  x {qty}")
        else:
            print("      (none)")
        
        # Calculate pricing with implied asks
        _, pricing = adapter.get_orderbook_with_pricing(market.ticker)
        if pricing:
            print(f"\n    PRICING:")
            print(f"      Yes: {pricing.best_yes_bid:.0f}¢ bid / {pricing.best_yes_ask:.0f}¢ ask  (spread: {pricing.yes_spread:.0f}¢)")
            print(f"      No:  {pricing.best_no_bid:.0f}¢ bid / {pricing.best_no_ask:.0f}¢ ask  (spread: {pricing.no_spread:.0f}¢)")
        
        print()
    
    print("=" * 70)
    print("✓ TEST COMPLETE")
    print("=" * 70)
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


#!/usr/bin/env python3
"""
Test: Fetch orderbook data for ALL markets in an event.

This verifies our adapter can:
1. Fetch all markets belonging to an event
2. Get orderbook data for each market
3. Parse pricing correctly

Usage:
    cd /Users/christiandiaz/Kalshi_Quant
    source venv/bin/activate
    PYTHONPATH=/Users/christiandiaz/Kalshi_Quant python kalshi_qete/tests/test_event_orderbooks.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter


def test_event_orderbooks(event_ticker: str, adapter: KalshiAdapter):
    """Test orderbook fetching for all markets in an event."""
    
    print(f"\n{'='*70}")
    print(f"EVENT: {event_ticker}")
    print(f"{'='*70}")
    
    # Get all markets for this event
    markets = adapter.get_markets_by_event(event_ticker)
    print(f"\nTotal markets in event: {len(markets)}")
    
    if not markets:
        print("  ✗ No markets found!")
        return 0, 0
    
    # Get orderbook for each market
    success = 0
    empty = 0
    
    print(f"\n{'Ticker':<30} {'Yes Lvls':>8} {'No Lvls':>8} {'Best Yes':>10} {'Best No':>10}")
    print("-" * 70)
    
    for market in markets:
        raw = adapter.get_orderbook(market.ticker)
        
        yes_levels = len(raw.yes_bids) if raw and raw.yes_bids else 0
        no_levels = len(raw.no_bids) if raw and raw.no_bids else 0
        
        best_yes = f"{raw.yes_bids[-1][0]}¢" if raw and raw.yes_bids else "-"
        best_no = f"{raw.no_bids[-1][0]}¢" if raw and raw.no_bids else "-"
        
        if yes_levels > 0 or no_levels > 0:
            success += 1
            status = "✓"
        else:
            empty += 1
            status = "○"
        
        print(f"{status} {market.ticker:<28} {yes_levels:>8} {no_levels:>8} {best_yes:>10} {best_no:>10}")
    
    print("-" * 70)
    print(f"\nResults: {success}/{len(markets)} markets have orderbook data ({100*success/len(markets):.0f}%)")
    
    return success, len(markets)


def main():
    print("=" * 70)
    print("EVENT ORDERBOOK TEST")
    print("Verifying orderbook data retrieval for ALL markets in events")
    print("=" * 70)
    
    # Create adapter
    print("\nConnecting to Kalshi API...")
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    print("✓ Connected")
    
    # Test multiple events
    events_to_test = [
        "KXFEDCHAIRNOM-29",   # Fed Chair nomination (23 markets)
        "KXPRESNOMD-28",      # 2028 Dem Presidential nominee (38 markets)
    ]
    
    total_success = 0
    total_markets = 0
    
    for event in events_to_test:
        success, total = test_event_orderbooks(event, adapter)
        total_success += success
        total_markets += total
    
    # Final summary
    print(f"\n{'='*70}")
    print("OVERALL SUMMARY")
    print(f"{'='*70}")
    print(f"Events tested: {len(events_to_test)}")
    print(f"Total markets: {total_markets}")
    print(f"Markets with orderbook data: {total_success}")
    print(f"Success rate: {100*total_success/total_markets:.0f}%")
    
    if total_success == total_markets:
        print("\n✓ ALL TESTS PASSED - 100% orderbook retrieval")
        return True
    else:
        print(f"\n⚠ {total_markets - total_success} markets missing orderbook data")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


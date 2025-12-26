#!/usr/bin/env python3
"""
Test: Market Scanner

Tests the market scanner's discovery, filtering, and sorting capabilities.

Usage:
    cd /Users/christiandiaz/Kalshi_Quant
    source venv/bin/activate
    PYTHONPATH=/Users/christiandiaz/Kalshi_Quant python kalshi_qete/tests/test_scanner.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter
from kalshi_qete.src.engine.scanner import MarketScanner


def test_scan_event():
    """Test scanning all markets in an event."""
    print("\n" + "=" * 60)
    print("TEST 1: Scan Event")
    print("=" * 60)
    
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    scanner = MarketScanner(adapter)
    
    event = "KXFEDCHAIRNOM-29"
    print(f"\nScanning event: {event}")
    
    markets = scanner.scan_event(event)
    print(f"✓ Found {len(markets)} markets")
    
    # Show top 5 by yes bid
    markets_with_yes = [m for m in markets if m.pricing and m.pricing.best_yes_bid > 0]
    markets_with_yes.sort(key=lambda m: m.pricing.best_yes_bid, reverse=True)
    
    print("\nTop 5 candidates by Yes bid:")
    for m in markets_with_yes[:5]:
        print(f"  {m.pricing.best_yes_bid:>3.0f}¢  {m.market.ticker}")
    
    return len(markets) > 0


def test_filter_two_sided():
    """Test filtering for two-sided markets."""
    print("\n" + "=" * 60)
    print("TEST 2: Filter Two-Sided Markets")
    print("=" * 60)
    
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    scanner = MarketScanner(adapter)
    
    event = "KXFEDCHAIRNOM-29"
    all_markets = scanner.scan_event(event)
    two_sided = scanner.filter_by_two_sided(all_markets)
    
    print(f"\nAll markets: {len(all_markets)}")
    print(f"Two-sided (YES + NO bids): {len(two_sided)}")
    
    if two_sided:
        print("\nTwo-sided markets:")
        for m in two_sided[:5]:
            yes_lvl = len(m.orderbook.yes_bids) if m.orderbook else 0
            no_lvl = len(m.orderbook.no_bids) if m.orderbook else 0
            print(f"  {m.market.ticker}: {yes_lvl} YES lvls, {no_lvl} NO lvls")
    
    return True


def test_sort_by_spread():
    """Test sorting by spread (tightest first)."""
    print("\n" + "=" * 60)
    print("TEST 3: Sort by Spread")
    print("=" * 60)
    
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    scanner = MarketScanner(adapter)
    
    event = "KXFEDCHAIRNOM-29"
    markets = scanner.scan_event(event)
    two_sided = scanner.filter_by_two_sided(markets)
    sorted_markets = scanner.sort_by_spread(two_sided)
    
    print(f"\nMarkets sorted by spread (tightest first):")
    for m in sorted_markets[:5]:
        spread = m.pricing.yes_spread if m.pricing else None
        print(f"  {spread:>4.1f}¢  {m.market.ticker}")
    
    # Verify sorting
    spreads = [m.pricing.yes_spread for m in sorted_markets if m.pricing and m.pricing.yes_spread]
    is_sorted = all(spreads[i] <= spreads[i+1] for i in range(len(spreads)-1))
    print(f"\n✓ Correctly sorted: {is_sorted}")
    
    return is_sorted


def test_summarize_event():
    """Test event summary."""
    print("\n" + "=" * 60)
    print("TEST 4: Event Summary")
    print("=" * 60)
    
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    scanner = MarketScanner(adapter)
    
    event = "KXFEDCHAIRNOM-29"
    summary = scanner.summarize_event(event)
    
    print(f"\nEvent: {summary['event_ticker']}")
    print(f"  Total markets: {summary['total_markets']}")
    print(f"  With orderbook: {summary['markets_with_orderbook']}")
    print(f"  Two-sided: {summary['two_sided_markets']}")
    print(f"  Total volume (24h): ${summary['total_volume_24h']/100:,.2f}")
    print(f"\n  Leader: {summary['leader_ticker']}")
    print(f"  Leader price: {summary['leader_yes_bid']:.0f}¢")
    
    return summary['total_markets'] > 0


def test_create_snapshots():
    """Test creating database snapshots."""
    print("\n" + "=" * 60)
    print("TEST 5: Create Snapshots")
    print("=" * 60)
    
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    scanner = MarketScanner(adapter)
    
    event = "KXFEDCHAIRNOM-29"
    markets = scanner.scan_event(event)
    two_sided = scanner.filter_by_two_sided(markets)
    
    snapshots = scanner.create_snapshots(two_sided)
    
    print(f"\nCreated {len(snapshots)} snapshots from {len(two_sided)} two-sided markets")
    
    if snapshots:
        print("\nSample snapshot:")
        s = snapshots[0]
        print(f"  Ticker: {s.ticker}")
        print(f"  Timestamp: {s.snapshot_ts}")
        print(f"  Yes: {s.best_yes_bid}¢ / {s.best_yes_ask}¢")
        print(f"  No: {s.best_no_bid}¢ / {s.best_no_ask}¢")
        print(f"  Depth: {s.yes_bid_depth} yes, {s.no_bid_depth} no")
    
    return len(snapshots) > 0


def test_top_volume():
    """Test getting top markets by volume."""
    print("\n" + "=" * 60)
    print("TEST 6: Top Markets by Volume")
    print("=" * 60)
    
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    scanner = MarketScanner(adapter)
    
    markets = scanner.scan_top_volume(n=5, min_volume=100)
    
    print(f"\nTop 5 markets by 24h volume:")
    for i, m in enumerate(markets, 1):
        vol = m.market.volume_24h / 100
        print(f"  {i}. ${vol:>8,.2f}  {m.market.ticker}")
    
    # Verify sorted by volume
    volumes = [m.market.volume_24h for m in markets]
    is_sorted = all(volumes[i] >= volumes[i+1] for i in range(len(volumes)-1))
    print(f"\n✓ Correctly sorted by volume: {is_sorted}")
    
    return len(markets) > 0


def main():
    print("=" * 60)
    print("MARKET SCANNER TEST SUITE")
    print("=" * 60)
    
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    print(f"\n✓ Connected to Kalshi API")
    
    # Run tests
    results = {
        "scan_event": test_scan_event(),
        "filter_two_sided": test_filter_two_sided(),
        "sort_by_spread": test_sort_by_spread(),
        "summarize_event": test_summarize_event(),
        "create_snapshots": test_create_snapshots(),
        "top_volume": test_top_volume(),
    }
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test, result in results.items():
        status = "PASSED" if result else "FAILED"
        print(f"  {test}: {status}")
    
    print(f"\n{passed}/{total} tests passed")
    
    if passed == total:
        print("\n✓ ALL TESTS PASSED")
        return True
    else:
        print("\n✗ SOME TESTS FAILED")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


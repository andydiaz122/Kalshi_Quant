#!/usr/bin/env python3
"""
Test: Orderbook Parser Utilities

Tests the orderbook parsing functions with both mock data and live API data.

Usage:
    cd /Users/christiandiaz/Kalshi_Quant
    source venv/bin/activate
    PYTHONPATH=/Users/christiandiaz/Kalshi_Quant python kalshi_qete/tests/test_orderbook_parser.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter
from kalshi_qete.src.utils.orderbook import (
    extract_best_prices,
    calculate_depth_at_price,
    calculate_vwap,
    analyze_orderbook,
    format_orderbook_display,
)


def test_with_mock_data():
    """Test parser functions with known mock data."""
    print("\n" + "=" * 60)
    print("TEST 1: Mock Data")
    print("=" * 60)
    
    # Mock orderbook: YES bids and NO bids (sorted low→high)
    yes_bids = [
        [40, 100],   # 40¢ x 100 contracts
        [42, 50],    # 42¢ x 50 contracts
        [44, 75],    # 44¢ x 75 contracts
        [45, 200],   # 45¢ x 200 contracts (BEST BID)
    ]
    
    no_bids = [
        [48, 100],   # 48¢ x 100 contracts
        [50, 75],    # 50¢ x 75 contracts
        [52, 150],   # 52¢ x 150 contracts (BEST BID)
    ]
    
    print("\nMock Orderbook:")
    print(f"  YES bids: {yes_bids}")
    print(f"  NO bids:  {no_bids}")
    
    # Test 1a: Extract best prices
    print("\n--- extract_best_prices() ---")
    pricing = extract_best_prices(yes_bids, no_bids)
    
    assert pricing.best_yes_bid == 45.0, f"Expected 45, got {pricing.best_yes_bid}"
    assert pricing.best_no_bid == 52.0, f"Expected 52, got {pricing.best_no_bid}"
    assert pricing.best_yes_ask == 48.0, f"Expected 48 (100-52), got {pricing.best_yes_ask}"
    assert pricing.best_no_ask == 55.0, f"Expected 55 (100-45), got {pricing.best_no_ask}"
    assert pricing.yes_spread == 3.0, f"Expected 3, got {pricing.yes_spread}"
    
    print(f"  ✓ Best Yes Bid: {pricing.best_yes_bid}¢")
    print(f"  ✓ Best Yes Ask: {pricing.best_yes_ask}¢ (implied)")
    print(f"  ✓ Yes Spread: {pricing.yes_spread}¢")
    print(f"  ✓ Best No Bid: {pricing.best_no_bid}¢")
    print(f"  ✓ Best No Ask: {pricing.best_no_ask}¢ (implied)")
    print(f"  ✓ No Spread: {pricing.no_spread}¢")
    
    # Test 1b: Depth at price
    print("\n--- calculate_depth_at_price() ---")
    yes_depth_3c = calculate_depth_at_price(yes_bids, depth_cents=3)
    # Within 3¢ of 45: prices 42,43,44,45 → 42(50) + 44(75) + 45(200) = 325
    assert yes_depth_3c == 325, f"Expected 325, got {yes_depth_3c}"
    print(f"  ✓ YES depth within 3¢: {yes_depth_3c} contracts")
    
    no_depth_5c = calculate_depth_at_price(no_bids, depth_cents=5)
    # Within 5¢ of 52: prices 48,49,50,51,52 → all three = 100+75+150 = 325
    assert no_depth_5c == 325, f"Expected 325, got {no_depth_5c}"
    print(f"  ✓ NO depth within 5¢: {no_depth_5c} contracts")
    
    # Test 1c: VWAP
    print("\n--- calculate_vwap() ---")
    yes_vwap = calculate_vwap(yes_bids, max_levels=2)
    # Top 2: 45¢ x 200, 44¢ x 75
    # VWAP = (45*200 + 44*75) / (200+75) = (9000+3300) / 275 = 44.73
    expected_vwap = (45*200 + 44*75) / (200+75)
    assert abs(yes_vwap - expected_vwap) < 0.01, f"Expected {expected_vwap:.2f}, got {yes_vwap:.2f}"
    print(f"  ✓ YES VWAP (top 2 levels): {yes_vwap:.2f}¢")
    
    # Test 1d: Full analysis
    print("\n--- analyze_orderbook() ---")
    analysis = analyze_orderbook(yes_bids, no_bids)
    print(f"  ✓ Mid price: {analysis['mid_price']:.2f}¢")
    print(f"  ✓ Imbalance: {analysis['imbalance']:+.3f} ({'YES' if analysis['imbalance'] > 0 else 'NO'} pressure)")
    print(f"  ✓ YES levels: {analysis['yes_levels']}, NO levels: {analysis['no_levels']}")
    
    print("\n✓ All mock data tests PASSED")
    return True


def test_with_live_data():
    """Test parser functions with real API data."""
    print("\n" + "=" * 60)
    print("TEST 2: Live API Data")
    print("=" * 60)
    
    # Connect to API
    print("\nConnecting to Kalshi API...")
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    print("✓ Connected")
    
    # Find a market with good liquidity (Kevin Hassett had both sides)
    ticker = "KXFEDCHAIRNOM-29-KH"
    print(f"\nFetching orderbook for {ticker}...")
    
    raw = adapter.get_orderbook(ticker)
    if not raw or (not raw.yes_bids and not raw.no_bids):
        print(f"⚠ Market {ticker} has no orderbook data, trying another...")
        # Try to find any market with data
        markets = adapter.get_markets_by_event("KXFEDCHAIRNOM-29")
        for m in markets:
            raw = adapter.get_orderbook(m.ticker)
            if raw and raw.yes_bids and raw.no_bids:
                ticker = m.ticker
                print(f"✓ Found market with data: {ticker}")
                break
    
    if not raw or (not raw.yes_bids and not raw.no_bids):
        print("⚠ Could not find market with orderbook data")
        return True  # Not a failure, just no data available
    
    print(f"✓ Got orderbook: {len(raw.yes_bids)} YES levels, {len(raw.no_bids)} NO levels")
    
    # Display formatted orderbook
    print("\n--- Formatted Orderbook ---")
    print(format_orderbook_display(raw.yes_bids, raw.no_bids, levels=5))
    
    # Run full analysis
    print("\n--- Full Analysis ---")
    analysis = analyze_orderbook(raw.yes_bids, raw.no_bids)
    
    for key, value in analysis.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    print("\n✓ Live data test PASSED")
    return True


def main():
    print("=" * 60)
    print("ORDERBOOK PARSER TEST SUITE")
    print("=" * 60)
    
    # Run tests
    mock_ok = test_with_mock_data()
    live_ok = test_with_live_data()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Mock data tests: {'PASSED' if mock_ok else 'FAILED'}")
    print(f"  Live data tests: {'PASSED' if live_ok else 'FAILED'}")
    
    if mock_ok and live_ok:
        print("\n✓ ALL TESTS PASSED")
        return True
    else:
        print("\n✗ SOME TESTS FAILED")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


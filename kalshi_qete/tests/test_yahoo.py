#!/usr/bin/env python3
"""
Test Suite: Yahoo Finance Adapter

Verifies that we can:
1. Fetch live Treasury yield data asynchronously
2. Get historical data for rolling windows
3. Calculate z-scores correctly
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

# Add project to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete.src.adapters.yahoo import (
    YahooAdapter, 
    PriceSnapshot, 
    HistoricalData,
    get_treasury_yield,
    get_treasury_z_score
)


def print_header(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def print_result(test_name: str, passed: bool, details: str = ""):
    """Print test result."""
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"  {status} | {test_name}")
    if details:
        print(f"         | {details}")


async def test_live_price():
    """Test 1: Fetch live Treasury yield."""
    print_header("Test 1: Live Treasury Yield (^IRX)")
    
    adapter = YahooAdapter()
    
    try:
        # Get live price
        price = await adapter.get_live_price("^IRX")
        
        # Validate
        passed = price is not None and 0 < price < 20  # Yields typically 0-20%
        print_result(
            "Get live ^IRX yield",
            passed,
            f"Yield: {price:.4f}%"
        )
        
        # Test caching (second call should be instant)
        import time
        start = time.time()
        cached_price = await adapter.get_live_price("^IRX")
        elapsed = time.time() - start
        
        print_result(
            "Cache hit (should be < 0.1s)",
            elapsed < 0.1,
            f"Elapsed: {elapsed:.4f}s, Cached: {cached_price:.4f}%"
        )
        
        return price
        
    except Exception as e:
        print_result("Get live ^IRX yield", False, f"Error: {e}")
        return None


async def test_snapshot():
    """Test 2: Get full price snapshot with change data."""
    print_header("Test 2: Price Snapshot")
    
    adapter = YahooAdapter()
    
    try:
        snapshot = await adapter.get_snapshot("^IRX")
        
        passed = (
            snapshot is not None and
            isinstance(snapshot, PriceSnapshot) and
            snapshot.ticker == "^IRX" and
            snapshot.price > 0
        )
        
        print_result(
            "Get snapshot object",
            passed,
            str(snapshot)
        )
        
        # Check timestamp is recent
        age = (datetime.now() - snapshot.timestamp).total_seconds()
        print_result(
            "Timestamp is recent",
            age < 60,
            f"Age: {age:.1f}s"
        )
        
        return snapshot
        
    except Exception as e:
        print_result("Get snapshot", False, f"Error: {e}")
        return None


async def test_historical():
    """Test 3: Get historical data for rolling window."""
    print_header("Test 3: Historical Data")
    
    adapter = YahooAdapter()
    
    try:
        # Get 1-day of 1-minute data
        history = await adapter.get_history("^IRX", period="1d", interval="1m")
        
        passed = (
            history is not None and
            isinstance(history, HistoricalData) and
            len(history.prices) > 0
        )
        
        print_result(
            "Get historical data",
            passed,
            f"{len(history.prices)} data points"
        )
        
        # Check statistics
        print(f"\n  📊 Statistics:")
        print(f"     Latest:  {history.latest:.4f}%")
        print(f"     Mean:    {history.mean:.4f}%")
        print(f"     Std Dev: {history.std:.4f}%")
        print(f"     Min:     {min(history.prices):.4f}%")
        print(f"     Max:     {max(history.prices):.4f}%")
        
        return history
        
    except Exception as e:
        print_result("Get historical data", False, f"Error: {e}")
        return None


async def test_z_score():
    """Test 4: Z-Score calculation."""
    print_header("Test 4: Z-Score Calculation")
    
    adapter = YahooAdapter()
    
    try:
        # Get z-score
        z = await adapter.get_z_score("^IRX", period="1d", interval="1m")
        
        # Z-score should typically be between -3 and +3
        passed = -5 < z < 5
        
        print_result(
            "Calculate z-score",
            passed,
            f"Z-Score: {z:+.4f}"
        )
        
        # Interpret
        if abs(z) < 1:
            interpretation = "Normal (within 1 std dev)"
        elif abs(z) < 2:
            interpretation = "Slightly unusual (1-2 std devs)"
        else:
            direction = "SPIKE 📈" if z > 0 else "DROP 📉"
            interpretation = f"SIGNIFICANT {direction} (>2 std devs)"
        
        print(f"\n  📈 Interpretation: {interpretation}")
        
        return z
        
    except Exception as e:
        print_result("Calculate z-score", False, f"Error: {e}")
        return None


async def test_convenience_functions():
    """Test 5: Test convenience functions."""
    print_header("Test 5: Convenience Functions")
    
    try:
        # get_treasury_yield
        yield_val = await get_treasury_yield()
        print_result(
            "get_treasury_yield()",
            yield_val > 0,
            f"Yield: {yield_val:.4f}%"
        )
        
        # get_treasury_z_score
        z = await get_treasury_z_score(period="1d")
        print_result(
            "get_treasury_z_score()",
            -5 < z < 5,
            f"Z-Score: {z:+.4f}"
        )
        
        return True
        
    except Exception as e:
        print_result("Convenience functions", False, f"Error: {e}")
        return False


async def test_other_tickers():
    """Test 6: Test other common tickers."""
    print_header("Test 6: Other Tickers")
    
    adapter = YahooAdapter()
    
    tickers = [
        ("^TNX", "10-Year Treasury"),
        ("^VIX", "VIX Volatility"),
        ("SPY", "S&P 500 ETF"),
    ]
    
    for ticker, name in tickers:
        try:
            price = await adapter.get_live_price(ticker)
            print_result(
                f"{name} ({ticker})",
                price is not None,
                f"Price: {price:.4f}"
            )
        except Exception as e:
            print_result(f"{name} ({ticker})", False, f"Error: {e}")


async def run_all_tests():
    """Run all tests."""
    print("\n" + "🧪 " * 10)
    print("   YAHOO FINANCE ADAPTER TEST SUITE")
    print("🧪 " * 10)
    print(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run tests
    yield_val = await test_live_price()
    snapshot = await test_snapshot()
    history = await test_historical()
    z_score = await test_z_score()
    await test_convenience_functions()
    await test_other_tickers()
    
    # Summary
    print_header("SUMMARY")
    
    if yield_val and snapshot and history and z_score is not None:
        print("  ✅ All core tests passed!")
        print(f"\n  📊 Current Treasury State:")
        print(f"     ^IRX Yield:    {yield_val:.4f}%")
        print(f"     Rolling Mean:  {history.mean:.4f}%")
        print(f"     Z-Score:       {z_score:+.4f}")
        
        # Trading signal preview
        if z_score > 2.0:
            print(f"\n  ⚠️  SIGNAL: Yield SPIKE detected!")
            print(f"     Consider: BUY Fed Hike / SELL Fed Cut on Kalshi")
        elif z_score < -2.0:
            print(f"\n  ⚠️  SIGNAL: Yield DROP detected!")
            print(f"     Consider: BUY Fed Cut / SELL Fed Hike on Kalshi")
        else:
            print(f"\n  ℹ️  No significant yield movement detected")
            print(f"     (Z-score within ±2 std devs)")
    else:
        print("  ❌ Some tests failed - check output above")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    asyncio.run(run_all_tests())


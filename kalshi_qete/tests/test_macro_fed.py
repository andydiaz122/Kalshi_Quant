#!/usr/bin/env python3
"""
Test Suite: Macro Fed Correlation Strategy

Tests the correlation between Treasury yields and Kalshi Fed markets.
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

# Add project to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.adapters.yahoo import YahooAdapter
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter
from kalshi_qete.src.strategies.macro_fed import (
    MacroFedStrategy,
    YieldSnapshot,
    FedMarketSnapshot,
    FedAction,
    run_macro_fed_strategy,
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


async def test_yield_snapshot():
    """Test 1: Get yield snapshot with z-score."""
    print_header("Test 1: Yield Snapshot")
    
    yahoo = YahooAdapter()
    kalshi = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    
    strategy = MacroFedStrategy(yahoo, kalshi)
    
    try:
        snapshot = await strategy.get_yield_snapshot("^IRX")
        
        passed = (
            snapshot is not None and
            isinstance(snapshot, YieldSnapshot) and
            snapshot.current_yield > 0 and
            snapshot.data_points > 0
        )
        
        print_result(
            "Get yield snapshot",
            passed,
            str(snapshot)
        )
        
        # Test z-score interpretation
        print(f"\n  📊 Z-Score Analysis:")
        print(f"     Current Yield: {snapshot.current_yield:.4f}%")
        print(f"     Mean Yield:    {snapshot.mean_yield:.4f}%")
        print(f"     Std Dev:       {snapshot.std_dev:.4f}%")
        print(f"     Z-Score:       {snapshot.z_score:+.4f}")
        print(f"     Is Spike:      {snapshot.is_spike}")
        print(f"     Is Dump:       {snapshot.is_dump}")
        print(f"     Signal:        {snapshot.signal_direction}")
        
        return snapshot
        
    except Exception as e:
        print_result("Get yield snapshot", False, f"Error: {e}")
        return None


async def test_fed_markets():
    """Test 2: Scan and categorize Fed markets."""
    print_header("Test 2: Fed Markets Scan")
    
    yahoo = YahooAdapter()
    kalshi = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    
    strategy = MacroFedStrategy(yahoo, kalshi)
    
    try:
        snapshot = await strategy.get_fed_markets()
        
        total_markets = (
            len(snapshot.hike_markets) + 
            len(snapshot.cut_markets) + 
            len(snapshot.hold_markets)
        )
        
        passed = snapshot is not None and total_markets >= 0
        
        print_result(
            "Get Fed markets",
            passed,
            f"Found {total_markets} total markets"
        )
        
        print(f"\n  📊 Market Categories:")
        print(f"     Hike Markets: {len(snapshot.hike_markets)}")
        print(f"     Cut Markets:  {len(snapshot.cut_markets)}")
        print(f"     Hold Markets: {len(snapshot.hold_markets)}")
        print(f"\n  📈 Implied Probabilities:")
        print(f"     Avg Hike: {snapshot.total_hike_prob:.1f}¢")
        print(f"     Avg Cut:  {snapshot.total_cut_prob:.1f}¢")
        print(f"     Avg Hold: {snapshot.hold_prob:.1f}¢")
        
        # Show sample markets
        if snapshot.hike_markets:
            print(f"\n  Sample Hike Markets:")
            for m in snapshot.hike_markets[:3]:
                print(f"     - {m.market.ticker}: {m.market.title[:50]}...")
        
        if snapshot.cut_markets:
            print(f"\n  Sample Cut Markets:")
            for m in snapshot.cut_markets[:3]:
                print(f"     - {m.market.ticker}: {m.market.title[:50]}...")
        
        return snapshot
        
    except Exception as e:
        print_result("Get Fed markets", False, f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


async def test_strategy_run():
    """Test 3: Run full strategy."""
    print_header("Test 3: Full Strategy Run")
    
    yahoo = YahooAdapter()
    kalshi = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    
    strategy = MacroFedStrategy(yahoo, kalshi, z_score_threshold=2.0)
    
    try:
        signals = await strategy.run()
        
        print_result(
            "Run strategy",
            True,
            f"Generated {len(signals)} signals"
        )
        
        # Show state
        state = strategy.get_state()
        print(f"\n  📊 Strategy State:")
        print(f"     Last Yield: {state['last_yield']}")
        print(f"     Last Fed Snapshot: {state['last_fed_snapshot']}")
        print(f"     Signal Count: {state['signal_count']}")
        
        # Show signals if any
        if signals:
            print(f"\n  📣 Generated Signals:")
            for i, cs in enumerate(signals, 1):
                print(f"     {i}. {cs.signal.side.value} {cs.signal.ticker} @ {cs.signal.price}¢")
                print(f"        Rationale: {cs.rationale}")
        else:
            print(f"\n  ℹ️  No signals generated (yield within normal range)")
        
        return signals
        
    except Exception as e:
        print_result("Run strategy", False, f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


async def test_convenience_runner():
    """Test 4: Test convenience runner function."""
    print_header("Test 4: Convenience Runner")
    
    try:
        signals = await run_macro_fed_strategy(
            key_id=config.KEY_ID,
            key_file_path=str(config.KEY_FILE_PATH),
            z_threshold=2.0,
            verbose=True,
        )
        
        print_result(
            "Convenience runner",
            True,
            f"Generated {len(signals)} signals"
        )
        
        return signals
        
    except Exception as e:
        print_result("Convenience runner", False, f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


async def run_all_tests():
    """Run all tests."""
    print("\n" + "🏦 " * 10)
    print("   MACRO FED STRATEGY TEST SUITE")
    print("🏦 " * 10)
    print(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run tests
    yield_snap = await test_yield_snapshot()
    fed_snap = await test_fed_markets()
    signals = await test_strategy_run()
    
    # Summary
    print_header("SUMMARY")
    
    if yield_snap and fed_snap is not None:
        print("  ✅ Core tests passed!")
        print(f"\n  📊 Current State:")
        print(f"     ^IRX Yield:    {yield_snap.current_yield:.4f}%")
        print(f"     Z-Score:       {yield_snap.z_score:+.4f}")
        
        if yield_snap.is_spike:
            print(f"\n  ⚠️  YIELD SPIKE - Consider buying Fed hike contracts")
        elif yield_snap.is_dump:
            print(f"\n  ⚠️  YIELD DUMP - Consider buying Fed cut contracts")
        else:
            print(f"\n  ℹ️  Yield normal - No correlation signals expected")
    else:
        print("  ❌ Some tests failed - check output above")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    asyncio.run(run_all_tests())


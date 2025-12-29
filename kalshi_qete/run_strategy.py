#!/usr/bin/env python3
"""
QETE Strategy Runner - Complete Structural Arbitrage Scanner

Scans the Kalshi platform for REAL arbitrage opportunities by:
1. Finding top-volume markets (topic-agnostic: Sports, Politics, Econ, etc.)
2. Discovering ALL events from those markets
3. Fetching COMPLETE market data for each event
4. Analyzing complete data to eliminate false positives

This is the production-ready scanner that finds actual mispricings.

Usage:
    cd /Users/christiandiaz/Kalshi_Quant
    source venv/bin/activate
    PYTHONPATH=/Users/christiandiaz/Kalshi_Quant python kalshi_qete/run_strategy.py
    
    # With options:
    python kalshi_qete/run_strategy.py --top 100 --buy-threshold 97 --verbose
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Ensure imports work
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter
from kalshi_qete.src.strategies.structural_arb import (
    StructuralArbScanner,
    EventAnalysis,
    CompleteEventData,
)


def print_header():
    """Print the runner header."""
    print("=" * 70)
    print("🎯 QETE COMPLETE STRUCTURAL ARBITRAGE SCANNER")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: Complete Event Data Analysis (No False Positives)")
    print("=" * 70)


def print_classification_summary(excluded_events: dict, safe_count: int, total_count: int):
    """Print summary of event classification."""
    print(f"\n🔒 EVENT CLASSIFICATION (Safety Filter)")
    print("-" * 50)
    
    print(f"  Total events discovered: {total_count}")
    print(f"  ✓ Mutually Exclusive (SAFE): {safe_count}")
    print(f"  ✗ Independent/Unknown (EXCLUDED): {len(excluded_events)}")
    
    if excluded_events:
        print(f"\n  Excluded events (would cause losses if traded):")
        for event_ticker, reason in sorted(excluded_events.items()):
            print(f"    ✗ {event_ticker}: {reason}")


def print_discovery_summary(
    complete_events: dict,
    initial_market_count: int
):
    """Print summary of event discovery."""
    print(f"\n📊 SAFE EVENT DATA SUMMARY")
    print("-" * 50)
    
    total_markets = sum(e.total_markets for e in complete_events.values())
    total_with_pricing = sum(e.markets_with_pricing for e in complete_events.values())
    discovered = total_markets - initial_market_count
    
    print(f"  Safe events analyzed: {len(complete_events)}")
    print(f"  Total markets fetched: {total_markets}")
    print(f"  Markets with pricing: {total_with_pricing}")
    print(f"  Additional markets discovered: {discovered}")
    
    # Show events with significant discovery
    significant_discoveries = [
        (et, e) for et, e in complete_events.items()
        if e.total_markets > e.source_market_count * 1.5 or 
           e.total_markets - e.source_market_count >= 3
    ]
    
    if significant_discoveries:
        print(f"\n  📈 Events with significant market discovery:")
        for et, e in sorted(significant_discoveries, key=lambda x: -x[1].total_markets):
            discovered = e.total_markets - e.source_market_count
            print(f"    {et}: {e.source_market_count} → {e.total_markets} (+{discovered})")


def print_completeness_report(complete_events: dict, verbose: bool = False):
    """Print data completeness report."""
    print(f"\n📋 DATA COMPLETENESS")
    print("-" * 50)
    
    full_coverage = []
    partial_coverage = []
    low_coverage = []
    
    for et, data in complete_events.items():
        coverage = data.completeness
        if coverage >= 0.9:
            full_coverage.append((et, data))
        elif coverage >= 0.5:
            partial_coverage.append((et, data))
        else:
            low_coverage.append((et, data))
    
    print(f"  ✓ Full coverage (>90%): {len(full_coverage)} events")
    print(f"  ⚠️ Partial coverage (50-90%): {len(partial_coverage)} events")
    print(f"  ❌ Low coverage (<50%): {len(low_coverage)} events")
    
    if verbose and partial_coverage:
        print(f"\n  Partial coverage details:")
        for et, data in partial_coverage[:5]:
            print(f"    {et}: {data.markets_with_pricing}/{data.total_markets} ({data.completeness*100:.0f}%)")
    
    if verbose and low_coverage:
        print(f"\n  Low coverage details (may not be accurate):")
        for et, data in low_coverage[:5]:
            print(f"    {et}: {data.markets_with_pricing}/{data.total_markets} ({data.completeness*100:.0f}%)")


def print_event_analysis(
    analyses: list, 
    complete_events: dict, 
    verbose: bool = False,
    min_coverage: float = 0.9,
    min_contracts: int = 10000
):
    """Print event analysis results with quality filtering."""
    print(f"\n📈 ARBITRAGE ANALYSIS (Complete Data)")
    print("-" * 50)
    
    # Separate by opportunity type
    buy_opps = [a for a in analyses if a.has_buy_arb]
    sell_opps = [a for a in analyses if a.has_sell_arb]
    
    # Filter to high-quality opportunities (volume is contract count)
    hq_buy = [a for a in buy_opps if a.is_high_quality(min_coverage, min_contracts)]
    hq_sell = [a for a in sell_opps if a.is_high_quality(min_coverage, min_contracts)]
    
    low_quality = [a for a in (buy_opps + sell_opps) if not a.is_high_quality(min_coverage, min_contracts)]
    
    close_to_buy = [a for a in analyses if not a.has_buy_arb and 98 <= a.sum_yes_asks < 103 and a.sum_yes_asks > 0]
    close_to_sell = [a for a in analyses if not a.has_sell_arb and 97 < a.sum_yes_bids <= 102]
    
    # Print HIGH QUALITY opportunities (ACTIONABLE)
    if hq_buy or hq_sell:
        print(f"\n🏆 HIGH QUALITY OPPORTUNITIES (Coverage ≥{min_coverage*100:.0f}%, Contracts ≥{min_contracts:,})")
        print("   ⚡ These are ACTIONABLE signals")
        
        for a in sorted(hq_buy, key=lambda x: x.sum_yes_asks):
            profit_pct = (100 - a.sum_yes_asks) / a.sum_yes_asks * 100 if a.sum_yes_asks > 0 else 0
            
            print(f"\n   🎯 BUY ALL: {a.event_ticker}")
            print(f"      Coverage: {a.coverage*100:.0f}% ({a.market_count}/{a.total_markets} markets)")
            print(f"      24h Contracts: {a.aggregate_volume:,}")
            print(f"      Sum(Asks): {a.sum_yes_asks:.1f}¢")
            print(f"      Profit: {a.buy_arb_profit:.1f}¢ per contract ({profit_pct:.1f}%)")
            
            if verbose:
                print(f"      Breakdown:")
                for m in a.markets:
                    ask = m.pricing.best_yes_ask if m.pricing else "N/A"
                    contracts = m.market.volume_24h or 0
                    print(f"        {m.market.ticker}: {ask}¢ ({contracts:,} contracts)")
        
        for a in sorted(hq_sell, key=lambda x: -x.sum_yes_bids):
            profit_pct = (a.sum_yes_bids - 100) / 100 * 100
            
            print(f"\n   🎯 SELL ALL: {a.event_ticker}")
            print(f"      Coverage: {a.coverage*100:.0f}% ({a.market_count}/{a.total_markets} markets)")
            print(f"      24h Contracts: {a.aggregate_volume:,}")
            print(f"      Sum(Bids): {a.sum_yes_bids:.1f}¢")
            print(f"      Profit: {a.sell_arb_profit:.1f}¢ per contract ({profit_pct:.1f}%)")
    else:
        print(f"\n   ✅ No high-quality arbitrage opportunities found")
        print(f"   (Markets are efficiently priced or opportunities don't meet quality thresholds)")
    
    # Print LOW QUALITY opportunities (filtered out)
    if low_quality:
        print(f"\n⚠️ LOW QUALITY OPPORTUNITIES ({len(low_quality)}) - FILTERED OUT:")
        for a in low_quality:
            reason = []
            if a.coverage < min_coverage:
                reason.append(f"coverage {a.coverage*100:.0f}% < {min_coverage*100:.0f}%")
            if a.aggregate_volume < min_contracts:
                reason.append(f"contracts {a.aggregate_volume:,} < {min_contracts:,}")
            
            arb_type = "BUY" if a.has_buy_arb else "SELL"
            profit = a.buy_arb_profit if a.has_buy_arb else a.sell_arb_profit
            print(f"   ✗ {a.event_ticker}: {arb_type} ({profit:.1f}¢) - {', '.join(reason)}")
    
    # Print near-misses
    if close_to_buy:
        print(f"\n👀 CLOSE TO BUY ARB (98-103¢):")
        for a in sorted(close_to_buy, key=lambda x: x.sum_yes_asks)[:5]:
            gap = a.sum_yes_asks - 98
            print(f"   {a.event_ticker}: {a.sum_yes_asks:.1f}¢ ({a.market_count} mkts, need {gap:.1f}¢ drop)")
    
    if close_to_sell:
        print(f"\n👀 CLOSE TO SELL ARB (97-102¢):")
        for a in sorted(close_to_sell, key=lambda x: -x.sum_yes_bids)[:5]:
            gap = 102 - a.sum_yes_bids
            print(f"   {a.event_ticker}: {a.sum_yes_bids:.1f}¢ ({a.market_count} mkts, need {gap:.1f}¢ rise)")


def print_signals(signal_groups: list):
    """Print generated signals."""
    if not signal_groups:
        print(f"\n📋 No trading signals generated (markets efficiently priced)")
        return
    
    print(f"\n📋 TRADING SIGNALS")
    print("-" * 50)
    print(f"\n   Signal Groups: {len(signal_groups)}")
    
    for sg in signal_groups:
        print(f"\n   {sg.group_name}")
        print(f"   Expected Profit: ${sg.expected_profit:.2f}")
        print(f"   Total Cost: ${sg.total_cost:.2f}")
        print(f"   Signals ({len(sg.signals)}):")
        for s in sg.signals:
            print(f"      {s.side} {s.size}x {s.ticker} @ {s.price}¢")


def print_statistics(analyses: list):
    """Print summary statistics."""
    print(f"\n📊 STATISTICS")
    print("-" * 50)
    
    if not analyses:
        print("   No data")
        return
    
    # Calculate distributions
    ask_sums = [a.sum_yes_asks for a in analyses if a.sum_yes_asks > 0]
    bid_sums = [a.sum_yes_bids for a in analyses if a.sum_yes_bids > 0]
    
    if ask_sums:
        print(f"\n   Sum(Asks) Distribution ({len(ask_sums)} events):")
        print(f"      Min: {min(ask_sums):.1f}¢")
        print(f"      Max: {max(ask_sums):.1f}¢")
        print(f"      Avg: {sum(ask_sums)/len(ask_sums):.1f}¢")
        
        # Buckets
        under_98 = len([x for x in ask_sums if x < 98])
        under_100 = len([x for x in ask_sums if x < 100])
        print(f"      <98¢ (BUY ARB): {under_98} ({under_98/len(ask_sums)*100:.0f}%)")
        print(f"      <100¢: {under_100} ({under_100/len(ask_sums)*100:.0f}%)")
    
    if bid_sums:
        print(f"\n   Sum(Bids) Distribution ({len(bid_sums)} events):")
        print(f"      Min: {min(bid_sums):.1f}¢")
        print(f"      Max: {max(bid_sums):.1f}¢")
        print(f"      Avg: {sum(bid_sums)/len(bid_sums):.1f}¢")
        
        # Buckets
        over_100 = len([x for x in bid_sums if x > 100])
        over_102 = len([x for x in bid_sums if x > 102])
        print(f"      >100¢: {over_100} ({over_100/len(bid_sums)*100:.0f}%)")
        print(f"      >102¢ (SELL ARB): {over_102} ({over_102/len(bid_sums)*100:.0f}%)")


def run_complete_scan(
    top_n: int = 100,
    min_volume: int = 100,
    buy_threshold: float = 98.0,
    sell_threshold: float = 102.0,
    min_coverage: float = 0.9,
    min_event_contracts: int = 10000,
    verbose: bool = False
):
    """
    Run the COMPLETE structural arbitrage scan.
    
    This fetches complete event data to eliminate false positives.
    
    Args:
        top_n: Number of top markets to scan for event discovery
        min_volume: Minimum 24h contract volume filter for initial scan
        buy_threshold: Sum of asks below this triggers buy signal
        sell_threshold: Sum of bids above this triggers sell signal
        min_coverage: Minimum coverage ratio for quality filter (default: 90%)
        min_event_contracts: Minimum aggregate 24h contract volume (default: 10,000)
        verbose: Print detailed output
    """
    print_header()
    
    print(f"\nConfiguration:")
    print(f"  Initial scan: Top {top_n} markets (min contracts: {min_volume})")
    print(f"  Buy threshold: <{buy_threshold}¢ (trigger BUY ALL)")
    print(f"  Sell threshold: >{sell_threshold}¢ (trigger SELL ALL)")
    print(f"  Quality filters: Coverage ≥{min_coverage*100:.0f}%, Contracts ≥{min_event_contracts:,}")
    print(f"  Mode: COMPLETE event data (no false positives)")
    
    # Initialize adapter
    print(f"\n🔌 Connecting to Kalshi API...")
    adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
    
    # Create complete scanner
    scanner = StructuralArbScanner(
        adapter=adapter,
        buy_threshold=buy_threshold,
        sell_threshold=sell_threshold,
        min_markets=2,
        max_markets=50,
        default_size=10
    )
    
    # Run complete scan
    print(f"\n🔍 Running complete structural arbitrage scan...")
    print(f"   (This fetches ALL markets for each discovered event)")
    
    analyses = scanner.scan_top_volume(n=top_n, min_volume=min_volume)
    
    # Get counts for stats
    total_events = len(scanner.complete_events) + len(scanner.excluded_events)
    safe_events = len(scanner.complete_events)
    initial_market_count = sum(
        e.source_market_count for e in scanner.complete_events.values()
    )
    
    # Print classification results
    print_classification_summary(scanner.excluded_events, safe_events, total_events)
    
    # Print safe event discovery
    print_discovery_summary(scanner.complete_events, initial_market_count)
    print_completeness_report(scanner.complete_events, verbose=verbose)
    print_event_analysis(
        analyses, 
        scanner.complete_events, 
        verbose=verbose,
        min_coverage=min_coverage,
        min_contracts=min_event_contracts
    )
    
    # Generate signals only for HIGH QUALITY opportunities
    hq_signal_groups = scanner.get_filtered_signal_groups(
        min_coverage=min_coverage,
        min_contracts=min_event_contracts
    )
    print_signals(hq_signal_groups)
    
    print_statistics(analyses)
    
    # Final summary
    print(f"\n" + "=" * 70)
    hq_opportunities = scanner.get_high_quality_opportunities(min_coverage, min_event_contracts)
    all_opportunities = scanner.get_opportunities()
    
    if hq_opportunities:
        total_profit = sum(
            a.buy_arb_profit if a.has_buy_arb else a.sell_arb_profit 
            for a in hq_opportunities
        )
        print(f"🏆 FOUND {len(hq_opportunities)} HIGH-QUALITY ARBITRAGE OPPORTUNITIES")
        print(f"   Total estimated profit: {total_profit:.1f}¢ per contract set")
        print(f"\n   ✅ Quality filters passed (Coverage ≥{min_coverage*100:.0f}%, Contracts ≥{min_event_contracts:,})")
        print(f"   📋 {len(hq_signal_groups)} signal groups ready for execution")
    elif all_opportunities:
        filtered_out = len(all_opportunities) - len(hq_opportunities)
        print(f"⚠️  FOUND {len(all_opportunities)} OPPORTUNITIES - ALL FILTERED OUT")
        print(f"   {filtered_out} opportunities failed quality filters")
        print(f"   (Coverage <{min_coverage*100:.0f}% or Contracts <{min_event_contracts:,})")
    else:
        print(f"✅ NO ARBITRAGE OPPORTUNITIES FOUND")
        print(f"   Markets are efficiently priced (Sum ≈ 100¢)")
        print(f"   Analyzed {len(analyses)} events with complete data")
    print("=" * 70)
    
    return scanner, analyses, hq_signal_groups


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="QETE Complete Structural Arbitrage Scanner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_strategy.py                    # Default scan (top 100)
  python run_strategy.py --top 200          # Wider scan
  python run_strategy.py --buy-threshold 97 # Tighter threshold
  python run_strategy.py --verbose          # Show detailed breakdown
  
Note: This scanner fetches COMPLETE event data to avoid false positives.
It will make many API calls (one per event discovered).
        """
    )
    
    parser.add_argument(
        "--top", "-n",
        type=int,
        default=100,
        help="Number of top markets to scan (default: 100)"
    )
    parser.add_argument(
        "--min-volume",
        type=int,
        default=100,
        help="Minimum 24h contract volume filter (default: 100)"
    )
    parser.add_argument(
        "--buy-threshold",
        type=float,
        default=98.0,
        help="Buy arb threshold in cents (default: 98)"
    )
    parser.add_argument(
        "--sell-threshold",
        type=float,
        default=102.0,
        help="Sell arb threshold in cents (default: 102)"
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.9,
        help="Minimum coverage ratio for quality filter (default: 0.9 = 90%%)"
    )
    parser.add_argument(
        "--min-event-contracts",
        type=int,
        default=10000,
        help="Minimum aggregate 24h contract volume (default: 10000)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output"
    )
    
    args = parser.parse_args()
    
    try:
        run_complete_scan(
            top_n=args.top,
            min_volume=args.min_volume,
            buy_threshold=args.buy_threshold,
            sell_threshold=args.sell_threshold,
            min_coverage=args.min_coverage,
            min_event_contracts=args.min_event_contracts,
            verbose=args.verbose
        )
    except KeyboardInterrupt:
        print("\n\nScan interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
QETE - Quantitative Event Trading Engine

Main entry point for the trading engine.

Usage:
    # Run with default settings (environment check + demo ingest)
    python kalshi_qete/main.py
    
    # Ingest a specific event
    python kalshi_qete/main.py --event KXFEDCHAIRNOM-29
    
    # Ingest a series
    python kalshi_qete/main.py --series KXHIGHNY
    
    # Ingest top volume markets
    python kalshi_qete/main.py --top-volume 50
    
    # Run continuous ingestion
    python kalshi_qete/main.py --continuous --interval 60
    
    # Run a trading strategy
    python kalshi_qete/main.py --strategy macro_fed
    python kalshi_qete/main.py --strategy structural_arb --top 50
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Ensure the package root is in the Python path
PACKAGE_ROOT = Path(__file__).parent.resolve()
PROJECT_ROOT = PACKAGE_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.utils.auth import validate_credentials


def verify_environment() -> bool:
    """
    Verify that the QETE environment is properly configured.
    
    Returns:
        True if environment is ready, False otherwise.
    """
    print("=" * 60)
    print("QETE - Quantitative Event Trading Engine")
    print("=" * 60)
    
    # Check directories exist
    print("\n[1/3] Checking directory structure...")
    dirs_ok = True
    for dir_path in [config.DATA_DIR, config.LOGS_DIR]:
        if dir_path.exists():
            print(f"  ✓ {dir_path.relative_to(PACKAGE_ROOT)}/")
        else:
            print(f"  ✗ {dir_path.relative_to(PACKAGE_ROOT)}/ (missing)")
            dirs_ok = False
    
    # Check credentials
    print("\n[2/3] Validating API credentials...")
    creds_ok = validate_credentials(config.KEY_ID, config.KEY_FILE_PATH)
    if creds_ok:
        print(f"  ✓ Key ID: {config.KEY_ID[:8]}...")
        print(f"  ✓ Key file: {config.KEY_FILE_PATH.name}")
    else:
        print(f"  ✗ Credentials not valid or key file missing")
        print(f"    Expected key file: {config.KEY_FILE_PATH}")
    
    # Check database path
    print("\n[3/3] Database configuration...")
    print(f"  → DB path: {config.DB_PATH.relative_to(PACKAGE_ROOT)}")
    
    print("\n" + "=" * 60)
    
    if dirs_ok and creds_ok:
        print("✓ Environment ready")
        return True
    else:
        print("✗ Environment needs configuration")
        return False


def run_demo_ingest():
    """Run a demo ingestion to verify the full pipeline."""
    from kalshi_qete.src.engine.ingest import IngestionPipeline
    
    print("\n" + "-" * 60)
    print("Demo Ingestion")
    print("-" * 60)
    
    with IngestionPipeline() as pipeline:
        # Test with Fed Chair nomination event
        result = pipeline.ingest_event("KXFEDCHAIRNOM-29", two_sided_only=True)
        
        print(f"\n  Event: KXFEDCHAIRNOM-29")
        print(f"  Status: {'SUCCESS' if result.success else 'FAILED'}")
        print(f"  Markets scanned: {result.markets_scanned}")
        print(f"  Snapshots stored: {result.snapshots_stored}")
        print(f"  Duration: {result.duration_seconds:.2f}s")
        
        # Show database stats
        stats = pipeline.store.get_stats()
        print(f"\n  Database Stats:")
        print(f"    Total snapshots: {stats['snapshot_count']}")
        print(f"    Unique tickers: {stats['unique_tickers']}")
        print(f"    DB size: {stats.get('db_size', 'N/A')}")


def run_event_ingest(event_ticker: str, two_sided: bool = True):
    """Ingest a specific event."""
    from kalshi_qete.src.engine.ingest import IngestionPipeline
    
    print(f"\n  Ingesting event: {event_ticker}")
    
    with IngestionPipeline() as pipeline:
        result = pipeline.ingest_event(event_ticker, two_sided_only=two_sided)
        print(f"\n  {result}")
        
        if result.errors:
            for error in result.errors:
                print(f"  ERROR: {error}")


def run_series_ingest(series_ticker: str):
    """Ingest a series."""
    from kalshi_qete.src.engine.ingest import IngestionPipeline
    
    print(f"\n  Ingesting series: {series_ticker}")
    
    with IngestionPipeline() as pipeline:
        result = pipeline.ingest_series(series_ticker)
        print(f"\n  {result}")


def run_top_volume_ingest(n: int, min_volume: int = 1000):
    """Ingest top volume markets."""
    from kalshi_qete.src.engine.ingest import IngestionPipeline
    
    print(f"\n  Ingesting top {n} markets by volume (min: {min_volume})")
    
    with IngestionPipeline() as pipeline:
        result = pipeline.ingest_top_volume(n=n, min_volume=min_volume)
        print(f"\n  {result}")


def run_continuous_ingest(events: list, interval: int):
    """Run continuous ingestion."""
    from kalshi_qete.src.engine.ingest import IngestionPipeline
    
    print(f"\n  Starting continuous ingestion")
    print(f"  Events: {events}")
    print(f"  Interval: {interval}s")
    print("  Press Ctrl+C to stop\n")
    
    def on_complete(result):
        print(f"    → {result}")
    
    with IngestionPipeline() as pipeline:
        pipeline.run_continuous(
            event_tickers=events,
            interval_seconds=interval,
            on_complete=on_complete
        )


def show_db_stats():
    """Show database statistics."""
    from kalshi_qete.src.db.duckdb_store import DuckDBStore
    
    print("\n" + "-" * 60)
    print("Database Statistics")
    print("-" * 60)
    
    with DuckDBStore(config.DB_PATH) as store:
        stats = store.get_stats()
        
        print(f"  Snapshots: {stats['snapshot_count']}")
        print(f"  Unique tickers: {stats['unique_tickers']}")
        print(f"  Unique series: {stats['unique_series']}")
        print(f"  Earliest: {stats['earliest_snapshot']}")
        print(f"  Latest: {stats['latest_snapshot']}")


# =============================================================================
# STRATEGY RUNNERS
# =============================================================================

def run_macro_fed_strategy(z_threshold: float = 2.0, verbose: bool = False):
    """
    Run the Macro Fed Correlation Strategy.
    
    Correlates Treasury yields with Kalshi Fed markets.
    """
    from kalshi_qete.src.strategies.macro_fed import run_macro_fed_strategy as _run
    
    asyncio.run(_run(
        key_id=config.KEY_ID,
        key_file_path=str(config.KEY_FILE_PATH),
        z_threshold=z_threshold,
        verbose=verbose,
    ))


def run_structural_arb_strategy(top_n: int = 50, verbose: bool = False):
    """
    Run the Structural Arbitrage Strategy.
    
    Scans mutually exclusive events for mispricing.
    """
    from kalshi_qete.run_strategy import run_complete_scan_async
    
    asyncio.run(run_complete_scan_async(
        top_n=top_n,
        min_volume=100,
        buy_threshold=98.0,
        sell_threshold=102.0,
        min_coverage=0.9,
        min_event_volume=5000.0,
        verbose=verbose,
    ))


AVAILABLE_STRATEGIES = {
    "macro_fed": {
        "fn": run_macro_fed_strategy,
        "desc": "Treasury yield / Kalshi Fed rate correlation",
    },
    "structural_arb": {
        "fn": run_structural_arb_strategy,
        "desc": "Mutually exclusive event mispricing",
    },
}


def list_strategies():
    """List available strategies."""
    print("\n" + "-" * 60)
    print("Available Strategies")
    print("-" * 60)
    for name, info in AVAILABLE_STRATEGIES.items():
        print(f"  {name}: {info['desc']}")


def main():
    """Main entry point for QETE."""
    parser = argparse.ArgumentParser(
        description="QETE - Quantitative Event Trading Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kalshi_qete/main.py                              # Run demo
  python kalshi_qete/main.py --event KXFEDCHAIRNOM-29     # Ingest event
  python kalshi_qete/main.py --series KXHIGHNY            # Ingest series
  python kalshi_qete/main.py --top-volume 50              # Ingest top 50
  python kalshi_qete/main.py --stats                      # Show DB stats
  python kalshi_qete/main.py --strategy macro_fed         # Run Fed correlation
  python kalshi_qete/main.py --strategy structural_arb    # Run arb scanner
  python kalshi_qete/main.py --list-strategies            # List strategies
        """
    )
    
    # Ingestion arguments
    parser.add_argument(
        "--event", "-e",
        help="Event ticker to ingest (e.g., KXFEDCHAIRNOM-29)"
    )
    parser.add_argument(
        "--series", "-s",
        help="Series ticker to ingest (e.g., KXHIGHNY)"
    )
    parser.add_argument(
        "--top-volume", "-t",
        type=int,
        help="Ingest top N markets by volume"
    )
    parser.add_argument(
        "--continuous", "-c",
        action="store_true",
        help="Run continuous ingestion"
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=60,
        help="Interval for continuous ingestion (seconds, default: 60)"
    )
    parser.add_argument(
        "--events",
        nargs="+",
        default=["KXFEDCHAIRNOM-29"],
        help="Events for continuous ingestion (default: KXFEDCHAIRNOM-29)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics"
    )
    
    # Strategy arguments
    parser.add_argument(
        "--strategy",
        choices=list(AVAILABLE_STRATEGIES.keys()),
        help="Run a trading strategy (macro_fed, structural_arb)"
    )
    parser.add_argument(
        "--list-strategies",
        action="store_true",
        help="List available strategies"
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=2.0,
        help="Z-score threshold for macro_fed strategy (default: 2.0)"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Number of top markets to scan for structural_arb (default: 50)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output for strategies"
    )
    
    # General arguments
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip environment verification"
    )
    
    args = parser.parse_args()
    
    # Handle strategy list first
    if args.list_strategies:
        list_strategies()
        return
    
    # Verify environment unless skipped
    if not args.skip_verify:
        env_ready = verify_environment()
        if not env_ready:
            print("\nPlease fix the configuration issues above.")
            print("Use --skip-verify to bypass this check.")
            sys.exit(1)
    
    # Route to appropriate command
    if args.strategy:
        # Run a trading strategy
        if args.strategy == "macro_fed":
            run_macro_fed_strategy(
                z_threshold=args.z_threshold,
                verbose=args.verbose,
            )
        elif args.strategy == "structural_arb":
            run_structural_arb_strategy(
                top_n=args.top,
                verbose=args.verbose,
            )
    elif args.stats:
        show_db_stats()
    elif args.event:
        run_event_ingest(args.event)
    elif args.series:
        run_series_ingest(args.series)
    elif args.top_volume:
        run_top_volume_ingest(args.top_volume)
    elif args.continuous:
        run_continuous_ingest(args.events, args.interval)
    else:
        # Default: run demo
        run_demo_ingest()
    
    print("\n✓ QETE completed successfully")


if __name__ == "__main__":
    main()

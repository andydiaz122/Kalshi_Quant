#!/usr/bin/env python3
"""
Test: DuckDB Storage Layer

Tests the database operations including:
- Schema creation
- Insert operations (single and batch)
- Query operations
- Integration with live API data

Usage:
    cd /Users/christiandiaz/Kalshi_Quant
    source venv/bin/activate
    PYTHONPATH=/Users/christiandiaz/Kalshi_Quant python kalshi_qete/tests/test_duckdb_store.py
"""

import sys
import tempfile
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete import config
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter
from kalshi_qete.src.db.duckdb_store import DuckDBStore
from kalshi_qete.src.db.models import OrderbookSnapshot
from kalshi_qete.src.engine.scanner import MarketScanner


def test_schema_creation():
    """Test database schema initialization."""
    print("\n" + "=" * 60)
    print("TEST 1: Schema Creation")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        store = DuckDBStore(db_path)
        
        # Verify tables exist
        tables = store.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        
        assert "orderbook_snapshots" in table_names, "orderbook_snapshots table missing"
        assert "market_metadata" in table_names, "market_metadata table missing"
        
        print(f"  ✓ Created tables: {table_names}")
        
        store.close()
    
    return True


def test_single_insert():
    """Test inserting a single snapshot."""
    print("\n" + "=" * 60)
    print("TEST 2: Single Insert")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        store = DuckDBStore(db_path)
        
        # Create test snapshot
        snapshot = OrderbookSnapshot(
            snapshot_ts=datetime.now(),
            ticker="TEST-TICKER-001",
            series_ticker="TEST",
            market_title="Test Market",
            best_yes_bid=45.0,
            best_yes_ask=47.0,
            best_no_bid=53.0,
            best_no_ask=55.0,
            yes_spread=2.0,
            no_spread=2.0,
            volume_24h=100000,
            yes_bid_depth=500,
            no_bid_depth=600,
        )
        
        # Insert
        store.insert_snapshot(snapshot)
        
        # Verify
        result = store.query_snapshots(ticker="TEST-TICKER-001")
        assert len(result) == 1, f"Expected 1 row, got {len(result)}"
        
        row = result.row(0, named=True)
        assert row["ticker"] == "TEST-TICKER-001"
        assert row["best_yes_bid"] == 45.0
        
        print(f"  ✓ Inserted and retrieved snapshot")
        print(f"    Ticker: {row['ticker']}")
        print(f"    Yes Bid: {row['best_yes_bid']}¢")
        
        store.close()
    
    return True


def test_batch_insert():
    """Test batch inserting multiple snapshots."""
    print("\n" + "=" * 60)
    print("TEST 3: Batch Insert")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        store = DuckDBStore(db_path)
        
        # Create multiple snapshots
        snapshots = []
        for i in range(10):
            snapshots.append(OrderbookSnapshot(
                snapshot_ts=datetime.now(),
                ticker=f"BATCH-TEST-{i:03d}",
                series_ticker="BATCH",
                market_title=f"Batch Test Market {i}",
                best_yes_bid=40.0 + i,
                best_yes_ask=42.0 + i,
                best_no_bid=58.0 - i,
                best_no_ask=60.0 - i,
                yes_spread=2.0,
                no_spread=2.0,
                volume_24h=10000 * (i + 1),
                yes_bid_depth=100 * (i + 1),
                no_bid_depth=100 * (i + 1),
            ))
        
        # Batch insert
        count = store.insert_snapshots(snapshots)
        assert count == 10, f"Expected 10 inserts, got {count}"
        
        # Query all
        result = store.query_snapshots(series_ticker="BATCH")
        assert len(result) == 10, f"Expected 10 rows, got {len(result)}"
        
        print(f"  ✓ Batch inserted {count} snapshots")
        print(f"  ✓ Query returned {len(result)} rows")
        
        store.close()
    
    return True


def test_live_data_integration():
    """Test with real API data."""
    print("\n" + "=" * 60)
    print("TEST 4: Live Data Integration")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        store = DuckDBStore(db_path)
        
        # Connect to API and scan an event
        print("  Connecting to Kalshi API...")
        adapter = KalshiAdapter(config.KEY_ID, config.KEY_FILE_PATH)
        scanner = MarketScanner(adapter)
        
        # Scan Fed Chair event
        print("  Scanning KXFEDCHAIRNOM-29 event...")
        markets = scanner.scan_event("KXFEDCHAIRNOM-29")
        two_sided = scanner.filter_by_two_sided(markets)
        
        print(f"  ✓ Found {len(two_sided)} two-sided markets")
        
        # Create snapshots
        snapshots = scanner.create_snapshots(two_sided)
        print(f"  ✓ Created {len(snapshots)} snapshots")
        
        # Insert into DB
        count = store.insert_snapshots(snapshots)
        print(f"  ✓ Inserted {count} rows into DuckDB")
        
        # Query and verify
        result = store.query_snapshots(series_ticker="KXFEDCHAIRNOM")
        print(f"  ✓ Query returned {len(result)} rows")
        
        # Get stats
        stats = store.get_stats()
        print(f"\n  Database Stats:")
        print(f"    Total snapshots: {stats['snapshot_count']}")
        print(f"    Unique tickers: {stats['unique_tickers']}")
        print(f"    Unique series: {stats['unique_series']}")
        
        # Test series summary query
        print("\n  Series Summary (top by yes bid):")
        summary = store.get_series_summary("KXFEDCHAIRNOM")
        for row in summary.head(5).iter_rows(named=True):
            print(f"    {row['ticker']}: {row['best_yes_bid']:.0f}¢ yes bid")
        
        store.close()
    
    return True


def test_query_operations():
    """Test various query operations."""
    print("\n" + "=" * 60)
    print("TEST 5: Query Operations")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        store = DuckDBStore(db_path)
        
        # Insert test data
        snapshots = []
        for i in range(5):
            snapshots.append(OrderbookSnapshot(
                snapshot_ts=datetime.now(),
                ticker=f"QUERY-TEST-{i:03d}",
                series_ticker="QUERYTEST",
                market_title=f"Query Test {i}",
                best_yes_bid=50.0 - i * 5,
                best_yes_ask=52.0 - i * 5,
                best_no_bid=48.0 + i * 5,
                best_no_ask=50.0 + i * 5,
                yes_spread=2.0,
                no_spread=2.0,
                volume_24h=50000 - i * 5000,
            ))
        
        store.insert_snapshots(snapshots)
        
        # Test get_latest_snapshot
        latest = store.get_latest_snapshot("QUERY-TEST-000")
        assert latest is not None, "get_latest_snapshot returned None"
        print(f"  ✓ get_latest_snapshot: {latest.row(0, named=True)['ticker']}")
        
        # Test query with limit
        limited = store.query_snapshots(limit=3)
        assert len(limited) == 3, f"Expected 3 rows with limit, got {len(limited)}"
        print(f"  ✓ query with limit=3: returned {len(limited)} rows")
        
        # Test series filter
        series_result = store.query_snapshots(series_ticker="QUERYTEST")
        assert len(series_result) == 5, f"Expected 5 rows for series, got {len(series_result)}"
        print(f"  ✓ query by series: returned {len(series_result)} rows")
        
        store.close()
    
    return True


def main():
    print("=" * 60)
    print("DUCKDB STORAGE LAYER TEST SUITE")
    print("=" * 60)
    
    results = {
        "schema_creation": test_schema_creation(),
        "single_insert": test_single_insert(),
        "batch_insert": test_batch_insert(),
        "query_operations": test_query_operations(),
        "live_data_integration": test_live_data_integration(),
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


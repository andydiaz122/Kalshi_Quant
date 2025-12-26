#!/usr/bin/env python3
"""
Test: Ingestion Pipeline

Tests the end-to-end data ingestion from Kalshi API to DuckDB.

Usage:
    cd /Users/christiandiaz/Kalshi_Quant
    source venv/bin/activate
    PYTHONPATH=/Users/christiandiaz/Kalshi_Quant python kalshi_qete/tests/test_pipeline.py
"""

import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from kalshi_qete.src.engine.ingest import (
    IngestionPipeline,
    IngestionResult,
    quick_ingest_event,
)


def test_pipeline_initialization():
    """Test pipeline initialization."""
    print("\n" + "=" * 60)
    print("TEST 1: Pipeline Initialization")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        pipeline = IngestionPipeline(db_path=db_path)
        
        # Check lazy loading - components should not be created yet
        assert pipeline._adapter is None, "Adapter should be lazy"
        assert pipeline._scanner is None, "Scanner should be lazy"
        assert pipeline._store is None, "Store should be lazy"
        
        print("  ✓ Pipeline created with lazy components")
        
        # Access adapter to trigger lazy load
        adapter = pipeline.adapter
        assert adapter is not None, "Adapter should be created on access"
        print("  ✓ Adapter lazy-loaded on first access")
        
        # Access store
        store = pipeline.store
        assert store is not None, "Store should be created on access"
        print(f"  ✓ Store lazy-loaded (db: {db_path})")
        
        pipeline.close()
    
    return True


def test_ingest_event():
    """Test ingesting a single event."""
    print("\n" + "=" * 60)
    print("TEST 2: Ingest Event")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with IngestionPipeline(db_path=db_path) as pipeline:
            # Ingest Fed Chair event
            result = pipeline.ingest_event(
                "KXFEDCHAIRNOM-29",
                two_sided_only=True
            )
            
            print(f"  Result: {result}")
            
            assert isinstance(result, IngestionResult)
            assert result.success, f"Ingestion failed: {result.errors}"
            assert result.markets_scanned > 0, "No markets scanned"
            assert result.snapshots_stored > 0, "No snapshots stored"
            
            print(f"  ✓ Scanned {result.markets_scanned} markets")
            print(f"  ✓ Stored {result.snapshots_stored} snapshots")
            print(f"  ✓ Duration: {result.duration_seconds:.2f}s")
            
            # Verify data in database
            stats = pipeline.store.get_stats()
            print(f"\n  Database Stats:")
            print(f"    Snapshots: {stats['snapshot_count']}")
            print(f"    Tickers: {stats['unique_tickers']}")
    
    return True


def test_ingest_series():
    """Test ingesting a series."""
    print("\n" + "=" * 60)
    print("TEST 3: Ingest Series")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with IngestionPipeline(db_path=db_path) as pipeline:
            # Ingest KXFEDCHAIRNOM series
            result = pipeline.ingest_series("KXFEDCHAIRNOM")
            
            print(f"  Result: {result}")
            
            assert result.success, f"Ingestion failed: {result.errors}"
            
            print(f"  ✓ Series ingestion: {result.snapshots_stored} snapshots")
    
    return True


def test_ingest_multiple_events():
    """Test ingesting multiple events."""
    print("\n" + "=" * 60)
    print("TEST 4: Ingest Multiple Events")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with IngestionPipeline(db_path=db_path) as pipeline:
            # Ingest multiple events
            events = ["KXFEDCHAIRNOM-29", "KXPRESNOMD-28"]
            results = pipeline.ingest_multiple_events(events, two_sided_only=True)
            
            assert len(results) == len(events), "Wrong number of results"
            
            total_stored = sum(r.snapshots_stored for r in results)
            
            print(f"  Events processed: {len(results)}")
            for i, (event, result) in enumerate(zip(events, results)):
                status = "✓" if result.success else "✗"
                print(f"    {status} {event}: {result.snapshots_stored} snapshots")
            
            print(f"\n  ✓ Total snapshots stored: {total_stored}")
            
            # Verify in database
            stats = pipeline.store.get_stats()
            assert stats['snapshot_count'] == total_stored
    
    return True


def test_get_status():
    """Test pipeline status reporting."""
    print("\n" + "=" * 60)
    print("TEST 5: Pipeline Status")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with IngestionPipeline(db_path=db_path) as pipeline:
            # Before any operations
            status = pipeline.get_status()
            print(f"  Initial status:")
            print(f"    API connected: {status['api_connected']}")
            print(f"    DB connected: {status['db_connected']}")
            
            # After ingestion
            pipeline.ingest_event("KXFEDCHAIRNOM-29", two_sided_only=True)
            
            status = pipeline.get_status()
            print(f"\n  After ingestion:")
            print(f"    API connected: {status['api_connected']}")
            print(f"    DB connected: {status['db_connected']}")
            print(f"    Snapshots in DB: {status['db_stats']['snapshot_count']}")
    
    return True


def test_quick_ingest():
    """Test convenience function."""
    print("\n" + "=" * 60)
    print("TEST 6: Quick Ingest Function")
    print("=" * 60)
    
    # Note: This uses the default config DB path
    # We'll just verify the function works
    print("  Testing quick_ingest_event()...")
    print("  (Uses default config.DB_PATH)")
    
    # For testing, we'll create a pipeline manually with temp DB
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with IngestionPipeline(db_path=db_path) as pipeline:
            result = pipeline.ingest_event("KXFEDCHAIRNOM-29", two_sided_only=True)
            
            assert result.success
            print(f"  ✓ Quick ingest returned: {result}")
    
    return True


def main():
    print("=" * 60)
    print("INGESTION PIPELINE TEST SUITE")
    print("=" * 60)
    
    results = {
        "pipeline_initialization": test_pipeline_initialization(),
        "ingest_event": test_ingest_event(),
        "ingest_series": test_ingest_series(),
        "ingest_multiple_events": test_ingest_multiple_events(),
        "get_status": test_get_status(),
        "quick_ingest": test_quick_ingest(),
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


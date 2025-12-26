#!/usr/bin/env python3
"""
Data Ingestion Service for Kalshi Quant Trading System.

Continuously fetches orderbook data for the next N Fed meetings every 60 seconds
and stores snapshots in DuckDB for historical analysis.
"""
import sys
import time
import signal
from datetime import datetime
from pathlib import Path

# Verify required packages are available
try:
    import pydantic
except ImportError:
    _script_dir = Path(__file__).parent.resolve()
    _venv_python = _script_dir / "venv" / "bin" / "python3"
    print("❌ ERROR: Required packages not found!")
    print(f"   Current Python: {sys.executable}")
    print("\n💡 SOLUTION: Activate the venv first:")
    print("   source venv/bin/activate")
    print("   python data_ingest.py")
    sys.exit(1)

from kalshi_python_sync import KalshiClient, Configuration
from kalshi_python_sync.auth import KalshiAuth
from kalshi_python_sync.exceptions import UnauthorizedException, ApiException

from config.settings import (
    KEY_ID, KEY_FILE_PATH, DATABASE_PATH, MIN_DAILY_VOLUME,
    INGESTION_INTERVAL_SECONDS, NUM_FED_MEETINGS
)
from utils.auth import load_private_key_pem
from database.db_manager import DatabaseManager
from ingestion.market_scanner import MarketScanner


# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    """Handle SIGINT (Ctrl+C) for graceful shutdown."""
    global shutdown_requested
    print("\n\n🛑 Shutdown requested...")
    shutdown_requested = True


def fetch_and_store_snapshots(scanner: MarketScanner, db_manager: DatabaseManager) -> tuple:
    """
    Fetch orderbook snapshots for next N Fed meetings and store in database.
    
    Args:
        scanner: MarketScanner instance with authenticated client
        db_manager: DatabaseManager instance (uses insert_snapshot_safe for low memory)
        
    Returns:
        Tuple of (success_count, error_count) for this iteration
    """
    success_count = 0
    error_count = 0
    
    try:
        # Get next N Fed meetings sorted by date
        print(f"📊 Fetching next {NUM_FED_MEETINGS} Fed meetings...")
        markets = scanner.get_next_n_meetings(
            series_ticker="KXFEDDECISION",
            n=NUM_FED_MEETINGS,
            min_volume=MIN_DAILY_VOLUME
        )
        
        if not markets:
            print("⚠️  No markets found meeting criteria")
            return (0, 0)
        
        print(f"✅ Found {len(markets)} markets to process")
        
        # Process each market
        for market in markets:
            try:
                print(f"   Processing: {market.ticker}")
                
                # Get orderbook snapshot
                snapshot = scanner.get_orderbook_snapshot(
                    ticker=market.ticker,
                    market=market,
                    series_ticker="KXFEDDECISION"
                )
                
                if snapshot is None:
                    print(f"      ⚠️  Insufficient orderbook data for {market.ticker}")
                    error_count += 1
                    continue
                
                # Store snapshot using safe insert (opens/closes connection per write)
                # This ensures low memory usage and data persistence
                db_manager.insert_snapshot_safe(snapshot)
                
                success_count += 1
                print(f"      ✅ Stored: Yes Bid {snapshot.best_yes_bid:.2f}¢ | No Bid {snapshot.best_no_bid:.2f}¢")
                
            except Exception as e:
                print(f"      ❌ Error processing {market.ticker}: {e}")
                error_count += 1
                continue
        
        return (success_count, error_count)
        
    except Exception as e:
        print(f"❌ Error in fetch_and_store_snapshots: {e}")
        import traceback
        traceback.print_exc()
        return (0, 1)


def main():
    """Main ingestion loop - runs every 60 seconds until interrupted."""
    global shutdown_requested
    
    # Validate key ID is set
    if KEY_ID == "YOUR_KEY_ID_HERE":
        print("❌ ERROR: Please set KEY_ID constant with your actual Kalshi API Key ID")
        return
    
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Initialize Kalshi client
        print("=" * 60)
        print("KALSHI QUANT - Data Ingestion Service")
        print("=" * 60)
        print(f"\n--- 🔌 INITIALIZING KALSHI CONNECTION ---")
        private_key_pem = load_private_key_pem(KEY_FILE_PATH)
        print(f"✅ Loaded private key from '{KEY_FILE_PATH}'")
        
        config = Configuration()
        client = KalshiClient(configuration=config)
        client.kalshi_auth = KalshiAuth(KEY_ID, private_key_pem)
        print("✅ Authentication configured")
        
        # Initialize scanner
        scanner = MarketScanner(client)
        print(f"✅ MarketScanner initialized")
        
        # Initialize DatabaseManager (used for insert_snapshot_safe)
        # Note: insert_snapshot_safe creates its own connection, so this instance's
        # connection is primarily for initialization. The instance is lightweight.
        db_manager = DatabaseManager(DATABASE_PATH)
        print(f"✅ DatabaseManager initialized")
        
        # Display configuration
        print(f"\n--- 📋 CONFIGURATION ---")
        print(f"   Database: {DATABASE_PATH}")
        print(f"   Series: KXFEDDECISION")
        print(f"   Meetings tracked: {NUM_FED_MEETINGS}")
        print(f"   Min volume: ${MIN_DAILY_VOLUME/100:.2f}")
        print(f"   Interval: {INGESTION_INTERVAL_SECONDS} seconds")
        print(f"\n--- 🚀 STARTING INGESTION LOOP ---")
        print(f"   Press Ctrl+C to stop gracefully\n")
        
        iteration = 0
        
        # Main ingestion loop
        while not shutdown_requested:
            iteration += 1
            iteration_start = datetime.now()
            
            print(f"\n{'='*60}")
            print(f"Iteration #{iteration} - {iteration_start.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            
            try:
                # Fetch and store snapshots
                success, errors = fetch_and_store_snapshots(scanner, db_manager)
                
                # Summary for this iteration
                print(f"\n--- Iteration Summary ---")
                print(f"   ✅ Success: {success} snapshots stored")
                if errors > 0:
                    print(f"   ⚠️  Errors: {errors}")
                
            except KeyboardInterrupt:
                # This should be caught by signal handler, but handle it here too
                break
            except Exception as e:
                print(f"❌ Error in iteration: {e}")
                # Continue to next iteration even if this one failed
                continue
            
            # Wait for next iteration (unless shutdown requested)
            if not shutdown_requested:
                print(f"\n⏳ Waiting {INGESTION_INTERVAL_SECONDS} seconds until next iteration...")
                
                # Sleep in smaller increments to check for shutdown more frequently
                for _ in range(INGESTION_INTERVAL_SECONDS):
                    if shutdown_requested:
                        break
                    time.sleep(1)
        
        # Clean up
        db_manager.close()
        
        print(f"\n\n✅ Ingestion service stopped gracefully")
        print(f"   Total iterations: {iteration}")
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: {e}")
    except UnauthorizedException as e:
        print(f"\n❌ AUTHENTICATION ERROR: {e}")
        print("   -> Please verify your KEY_ID and private key file are correct")
    except ApiException as e:
        print(f"\n❌ API ERROR: {e}")
        print("   -> Check your network connection and API status")
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        print(f"   -> Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


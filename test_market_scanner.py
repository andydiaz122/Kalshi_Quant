#!/usr/bin/env python3
"""
Test script for MarketScanner.get_next_n_meetings() method.
Tests that we can fetch and sort the next N Fed meetings.
"""
import sys
from pathlib import Path

# Verify required packages are available
try:
    from kalshi_python_sync import KalshiClient, Configuration
    from kalshi_python_sync.auth import KalshiAuth
except ImportError:
    print("❌ ERROR: kalshi_python_sync not found. Please install dependencies.")
    sys.exit(1)

from config.settings import KEY_ID, KEY_FILE_PATH
from utils.auth import load_private_key_pem
from ingestion.market_scanner import MarketScanner
from ingestion.market_date_parser import parse_ticker_date

def main():
    """Test the get_next_n_meetings method."""
    print("=" * 60)
    print("Testing MarketScanner.get_next_n_meetings()")
    print("=" * 60)
    
    try:
        # Initialize Kalshi client (same as main.py)
        print("\n--- 🔌 INITIALIZING KALSHI CONNECTION ---")
        private_key_pem = load_private_key_pem(KEY_FILE_PATH)
        print(f"✅ Loaded private key from '{KEY_FILE_PATH}'")
        
        config = Configuration()
        client = KalshiClient(configuration=config)
        client.kalshi_auth = KalshiAuth(KEY_ID, private_key_pem)
        print("✅ Authentication configured")
        
        # Create scanner
        scanner = MarketScanner(client)
        
        # Test get_next_n_meetings
        print("\n--- 📊 FETCHING NEXT 4 FED MEETINGS ---")
        next_meetings = scanner.get_next_n_meetings(
            series_ticker="KXFEDDECISION",
            n=4,
            min_volume=100000
        )
        
        if not next_meetings:
            print("⚠️  No meetings found")
            return
        
        print(f"✅ Found {len(next_meetings)} meetings\n")
        
        # Display results with parsed dates
        print("Next Fed Meetings (sorted by date, earliest first):")
        print("-" * 60)
        for i, market in enumerate(next_meetings, 1):
            date = parse_ticker_date(market.ticker)
            date_str = date.strftime('%Y-%m-%d') if date else "Unknown"
            print(f"{i}. {market.ticker:30} | Date: {date_str:10} | Volume: ${market.volume_24h/100:.2f}")
            print(f"   Title: {market.title[:70]}")
            print()
        
        print("✅ MarketScanner test complete!")
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: {e}")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()


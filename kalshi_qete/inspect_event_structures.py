#!/usr/bin/env python3
"""
Event Structure Inspector

Compares raw event metadata between:
- A "Real Arb" candidate (KXNEWPOPE - mutually exclusive: one pope)
- A "False Arb" candidate (KXTRUMPPARDONS - independent: multiple pardons possible)

Goal: Find if Kalshi provides a boolean flag or metadata field that indicates
whether an event's markets are mutually exclusive.

Usage:
    cd /Users/christiandiaz/Kalshi_Quant
    source venv/bin/activate
    python kalshi_qete/inspect_event_structures.py
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import urllib.request
import urllib.error

# Ensure imports work
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# Kalshi public API base URL (no auth needed for public endpoints)
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


def fetch_raw_event(event_ticker: str) -> dict:
    """
    Fetch raw event data using direct HTTP request.
    
    No authentication needed for public endpoints.
    """
    url = f"{BASE_URL}/events/{event_ticker}"
    
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data
    except urllib.error.HTTPError as e:
        print(f"HTTP Error fetching event {event_ticker}: {e.code} {e.reason}")
        return {"error": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        print(f"Error fetching event {event_ticker}: {e}")
        return {"error": str(e)}


def fetch_raw_markets_for_event(event_ticker: str, limit: int = 5) -> list:
    """
    Fetch raw market data for an event using direct HTTP request.
    """
    url = f"{BASE_URL}/markets?event_ticker={event_ticker}&limit={limit}"
    
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data.get('markets', [])
    except Exception as e:
        print(f"Error fetching markets for {event_ticker}: {e}")
        return []


def dump_event_comparison(event1_ticker: str, event2_ticker: str, output_file: str = None):
    """
    Fetch and compare two events' raw metadata.
    """
    print("=" * 70)
    print("🔍 EVENT STRUCTURE INSPECTOR")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nComparing:")
    print(f"  Event 1 (Expected Mutually Exclusive): {event1_ticker}")
    print(f"  Event 2 (Expected Independent): {event2_ticker}")
    print("=" * 70)
    
    # Fetch both events using direct HTTP (no auth needed)
    print(f"\n📥 Fetching {event1_ticker}...")
    event1_data = fetch_raw_event(event1_ticker)
    markets1 = fetch_raw_markets_for_event(event1_ticker, limit=3)
    
    print(f"📥 Fetching {event2_ticker}...")
    event2_data = fetch_raw_event(event2_ticker)
    markets2 = fetch_raw_markets_for_event(event2_ticker, limit=3)
    
    # Build comparison report
    report = {
        "inspection_timestamp": datetime.now().isoformat(),
        "purpose": "Find metadata fields indicating mutually exclusive markets",
        "events": {
            event1_ticker: {
                "label": "EXPECTED_MUTUALLY_EXCLUSIVE",
                "description": "Next Pope - exactly one winner",
                "raw_event_data": event1_data,
                "sample_markets": markets1
            },
            event2_ticker: {
                "label": "EXPECTED_INDEPENDENT",
                "description": "Trump Pardons - multiple can be true",
                "raw_event_data": event2_data,
                "sample_markets": markets2
            }
        }
    }
    
    # Print key fields side by side
    print("\n" + "=" * 70)
    print("📊 KEY FIELD COMPARISON")
    print("=" * 70)
    
    # Extract event objects
    e1 = event1_data.get('event', event1_data)
    e2 = event2_data.get('event', event2_data)
    
    # Fields to compare
    key_fields = [
        'title',
        'category',
        'sub_title',
        'mutually_exclusive',
        'series_ticker',
        'strike_type',
        'settlement_timer_seconds',
        'market_type',
        'collateral_return_type',
        'rules_primary',
    ]
    
    print(f"\n{'Field':<30} | {'Pope (ME?)':<35} | {'Pardons (Indep?)':<35}")
    print("-" * 105)
    
    for field in key_fields:
        v1 = e1.get(field, 'N/A')
        v2 = e2.get(field, 'N/A')
        
        # Truncate long values
        v1_str = str(v1)[:33] + "..." if len(str(v1)) > 35 else str(v1)
        v2_str = str(v2)[:33] + "..." if len(str(v2)) > 35 else str(v2)
        
        # Highlight differences
        marker = "🔍" if v1 != v2 else "  "
        print(f"{marker}{field:<28} | {v1_str:<35} | {v2_str:<35}")
    
    # Look for ANY fields that exist in one but not other
    print("\n" + "=" * 70)
    print("🔎 FIELDS UNIQUE TO EACH EVENT")
    print("=" * 70)
    
    e1_keys = set(e1.keys()) if isinstance(e1, dict) else set()
    e2_keys = set(e2.keys()) if isinstance(e2, dict) else set()
    
    only_in_e1 = e1_keys - e2_keys
    only_in_e2 = e2_keys - e1_keys
    
    if only_in_e1:
        print(f"\nFields only in {event1_ticker}:")
        for field in sorted(only_in_e1):
            print(f"  {field}: {e1.get(field)}")
    
    if only_in_e2:
        print(f"\nFields only in {event2_ticker}:")
        for field in sorted(only_in_e2):
            print(f"  {field}: {e2.get(field)}")
    
    if not only_in_e1 and not only_in_e2:
        print("\n  (Both events have the same fields)")
    
    # Dump full JSON
    print("\n" + "=" * 70)
    print(f"📄 FULL RAW JSON - {event1_ticker}")
    print("=" * 70)
    print(json.dumps(event1_data, indent=2, default=str))
    
    print("\n" + "=" * 70)
    print(f"📄 FULL RAW JSON - {event2_ticker}")
    print("=" * 70)
    print(json.dumps(event2_data, indent=2, default=str))
    
    # Sample market comparison
    print("\n" + "=" * 70)
    print("📄 SAMPLE MARKET COMPARISON")
    print("=" * 70)
    
    if markets1:
        print(f"\nFirst market from {event1_ticker}:")
        m1_keys = ['ticker', 'title', 'market_type', 'yes_sub_title', 'no_sub_title']
        for k in m1_keys:
            if k in markets1[0]:
                print(f"  {k}: {markets1[0][k]}")
    
    if markets2:
        print(f"\nFirst market from {event2_ticker}:")
        m2_keys = ['ticker', 'title', 'market_type', 'yes_sub_title', 'no_sub_title']
        for k in m2_keys:
            if k in markets2[0]:
                print(f"  {k}: {markets2[0][k]}")
    
    # Write to file if specified
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n✅ Full report saved to: {output_path}")
    
    # Analysis summary
    print("\n" + "=" * 70)
    print("📋 ANALYSIS SUMMARY")
    print("=" * 70)
    
    # Check for mutually_exclusive field
    me_flag_e1 = e1.get('mutually_exclusive')
    me_flag_e2 = e2.get('mutually_exclusive')
    
    if me_flag_e1 is not None or me_flag_e2 is not None:
        print("\n🎯 FOUND 'mutually_exclusive' FIELD!")
        print(f"   {event1_ticker}: {me_flag_e1}")
        print(f"   {event2_ticker}: {me_flag_e2}")
    else:
        print("\n❌ No 'mutually_exclusive' field found in event metadata")
    
    # Check category
    cat1 = e1.get('category')
    cat2 = e2.get('category')
    print(f"\n📂 Categories:")
    print(f"   {event1_ticker}: {cat1}")
    print(f"   {event2_ticker}: {cat2}")
    
    # Check series
    series1 = e1.get('series_ticker')
    series2 = e2.get('series_ticker')
    print(f"\n📊 Series:")
    print(f"   {event1_ticker}: {series1}")
    print(f"   {event2_ticker}: {series2}")
    
    return report


def main():
    """Main entry point."""
    # Events to compare
    mutually_exclusive_event = "KXNEWPOPE-70"  # Next Pope - one winner
    independent_event = "KXTRUMPPARDONS-29JAN21"  # Pardons - multiple possible
    
    # Output file
    output_file = PROJECT_ROOT / "kalshi_qete" / "data" / "event_structure_comparison.json"
    
    dump_event_comparison(
        mutually_exclusive_event,
        independent_event,
        output_file=str(output_file)
    )


if __name__ == "__main__":
    main()


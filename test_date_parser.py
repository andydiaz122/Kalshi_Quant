#!/usr/bin/env python3
"""
Quick test script for market_date_parser.py
Tests date parsing and sorting functionality.
"""
from ingestion.market_date_parser import parse_ticker_date, sort_markets_by_date
from datetime import datetime

# Test ticker formats we've seen
test_tickers = [
    "KXFEDDECISION-26JAN-H0",
    "KXFEDDECISION-15MAR-H0",
    "KXFEDDECISION-5FEB-H0",
    "KXFEDDECISION-30DEC-H0",
    "INVALID-TICKER",  # Should return None
    "KXFEDDECISION-26JAN",  # Missing suffix, should still work
]

print("=" * 60)
print("Testing parse_ticker_date()")
print("=" * 60)

for ticker in test_tickers:
    date = parse_ticker_date(ticker)
    if date:
        print(f"✅ {ticker:30} -> {date.strftime('%Y-%m-%d')}")
    else:
        print(f"❌ {ticker:30} -> None (invalid format)")

print("\n" + "=" * 60)
print("Testing sort_markets_by_date()")
print("=" * 60)

# Create mock Market objects for testing
class MockMarket:
    def __init__(self, ticker):
        self.ticker = ticker
        self.title = f"Market for {ticker}"

# Create markets with different dates
mock_markets = [
    MockMarket("KXFEDDECISION-30DEC-H0"),  # Latest
    MockMarket("KXFEDDECISION-5FEB-H0"),   # Middle
    MockMarket("KXFEDDECISION-26JAN-H0"),   # Earliest
    MockMarket("INVALID-TICKER"),           # Should sort last
]

sorted_markets = sort_markets_by_date(mock_markets)

print("Markets sorted by date (earliest first):")
for i, market in enumerate(sorted_markets, 1):
    date = parse_ticker_date(market.ticker)
    if date:
        print(f"  {i}. {market.ticker:30} -> {date.strftime('%Y-%m-%d')}")
    else:
        print(f"  {i}. {market.ticker:30} -> (unparseable, sorted last)")

print("\n✅ Date parser test complete!")


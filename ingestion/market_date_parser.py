"""
Market date parsing utilities for extracting and sorting Fed meeting dates from tickers.

Ticker format: KXFEDDECISION-26JAN-H0
- Series: KXFEDDECISION
- Date: 26JAN (day + month abbreviation)
- Suffix: H0 (additional identifier)
"""
import re
from datetime import datetime
from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from kalshi_python_sync.models.market import Market


def parse_ticker_date(ticker: str) -> Optional[datetime]:
    """
    Extract and parse date from market ticker.
    
    Ticker format: KXFEDDECISION-26JAN-H0
    Extracts "26JAN" and converts to datetime object.
    
    Args:
        ticker: Market ticker string (e.g., "KXFEDDECISION-26JAN-H0")
        
    Returns:
        datetime object representing the meeting date, or None if parsing fails
        
    Examples:
        >>> parse_ticker_date("KXFEDDECISION-26JAN-H0")
        datetime(2026, 1, 26)
        >>> parse_ticker_date("KXFEDDECISION-15MAR-H0")
        datetime(2026, 3, 15)
    """
    if not ticker or '-' not in ticker:
        return None
    
    # Split ticker by dashes: ["KXFEDDECISION", "26JAN", "H0"]
    parts = ticker.split('-')
    if len(parts) < 2:
        return None
    
    # Extract date portion (second part, e.g., "26JAN")
    date_str = parts[1]
    
    # Match pattern: 1-2 digits followed by 3-letter month abbreviation
    # Examples: "26JAN", "5FEB", "15MAR"
    match = re.match(r'^(\d{1,2})([A-Z]{3})$', date_str)
    if not match:
        return None
    
    day_str, month_str = match.groups()
    day = int(day_str)
    
    # Map month abbreviations to month numbers
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
        'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
        'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    
    month = month_map.get(month_str.upper())
    if month is None:
        return None
    
    # Determine year: assume current year or next year if month has passed
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # If the month is in the past this year, assume next year
    if month < current_month:
        year = current_year + 1
    else:
        year = current_year
    
    try:
        return datetime(year, month, day)
    except ValueError:
        # Invalid date (e.g., Feb 30)
        return None


def sort_markets_by_date(markets: List) -> List:
    """
    Sort markets by their meeting date (earliest first).
    Markets without parseable dates are placed at the end.
    
    Args:
        markets: List of objects with a 'ticker' attribute (e.g., Market objects)
        
    Returns:
        List of market objects sorted by date (ascending)
    """
    def get_sort_key(market) -> tuple:
        """
        Return a tuple for sorting: (date_timestamp, ticker).
        Markets without dates get a far-future timestamp to sort last.
        """
        date = parse_ticker_date(market.ticker)
        if date is None:
            # Use a far-future timestamp so unparseable dates sort last
            return (datetime(9999, 12, 31).timestamp(), market.ticker)
        return (date.timestamp(), market.ticker)
    
    return sorted(markets, key=get_sort_key)


"""
Orderbook Parsing Utilities

Functions for extracting pricing, calculating depth, and analyzing orderbook data.

Kalshi Orderbook Structure:
- API returns only BIDS (not asks) for both YES and NO sides
- Each side is a list of [price, quantity] pairs sorted low → high
- Best bid is the LAST element (highest price)

The Implied Ask Rule (binary market constraint):
- YES_ASK = 100 - BEST_NO_BID
- NO_ASK = 100 - BEST_YES_BID
- This works because YES + NO must equal 100¢

Example:
    If Best_Yes_Bid = 45¢ and Best_No_Bid = 52¢:
    - Implied_Yes_Ask = 100 - 52 = 48¢
    - Implied_No_Ask = 100 - 45 = 55¢
    - Yes_Spread = 48 - 45 = 3¢
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

from kalshi_qete.src.db.models import MarketPricing


def extract_best_prices(
    yes_bids: List[List[int]],
    no_bids: List[List[int]]
) -> Optional[MarketPricing]:
    """
    Extract best bid/ask prices from raw orderbook data.
    
    Args:
        yes_bids: List of [price, quantity] for YES bids (sorted low→high)
        no_bids: List of [price, quantity] for NO bids (sorted low→high)
        
    Returns:
        MarketPricing with best prices and spreads, or None if insufficient data
        
    Example:
        >>> yes_bids = [[40, 100], [42, 50], [45, 200]]  # Best = 45¢
        >>> no_bids = [[48, 100], [50, 75], [52, 150]]   # Best = 52¢
        >>> pricing = extract_best_prices(yes_bids, no_bids)
        >>> pricing.best_yes_bid
        45.0
        >>> pricing.best_yes_ask  # 100 - 52
        48.0
    """
    # Need at least one side to have bids
    if not yes_bids and not no_bids:
        return None
    
    # Best bid is LAST element (highest price) - Kalshi sorts low→high
    best_yes_bid = float(yes_bids[-1][0]) if yes_bids else None
    best_no_bid = float(no_bids[-1][0]) if no_bids else None
    
    # Calculate total depth (sum of all quantities)
    yes_depth = sum(level[1] for level in yes_bids) if yes_bids else 0
    no_depth = sum(level[1] for level in no_bids) if no_bids else 0
    
    # Need both sides for complete pricing
    if best_yes_bid is None or best_no_bid is None:
        # Return partial data
        return MarketPricing(
            best_yes_bid=best_yes_bid or 0.0,
            best_no_bid=best_no_bid or 0.0,
            yes_bid_depth=yes_depth,
            no_bid_depth=no_depth,
        )
    
    pricing = MarketPricing(
        best_yes_bid=best_yes_bid,
        best_no_bid=best_no_bid,
        yes_bid_depth=yes_depth,
        no_bid_depth=no_depth,
    )
    
    # Apply implied ask rule and calculate spreads
    pricing.calculate_implied_asks()
    pricing.calculate_spreads()
    
    return pricing


def calculate_depth_at_price(
    bids: List[List[int]],
    depth_cents: int = 5
) -> int:
    """
    Calculate total volume within X cents of best bid.
    
    Useful for measuring liquidity near the top of book.
    
    Args:
        bids: List of [price, quantity] pairs (sorted low→high)
        depth_cents: How many cents from best bid to include
        
    Returns:
        Total quantity within the specified depth
        
    Example:
        >>> bids = [[40, 100], [42, 50], [44, 75], [45, 200]]
        >>> calculate_depth_at_price(bids, depth_cents=3)
        325  # 44¢ (75) + 45¢ (200) = 275... wait let me recalc
        # 45-3=42, so include 42,44,45 = 50+75+200 = 325
    """
    if not bids:
        return 0
    
    best_bid = bids[-1][0]  # Last element is highest
    total_depth = 0
    
    # Iterate backwards from best bid
    for price, quantity in reversed(bids):
        if best_bid - price <= depth_cents:
            total_depth += quantity
        else:
            break  # Sorted, so no more within range
    
    return total_depth


def calculate_vwap(
    bids: List[List[int]],
    max_levels: int = 5
) -> Optional[float]:
    """
    Calculate Volume-Weighted Average Price for top N levels.
    
    VWAP gives a better sense of "fair price" than just best bid
    when there's thin liquidity at the top of book.
    
    Args:
        bids: List of [price, quantity] pairs (sorted low→high)
        max_levels: Number of levels from best bid to include
        
    Returns:
        VWAP in cents, or None if no bids
        
    Example:
        >>> bids = [[40, 100], [42, 50], [45, 200]]
        >>> calculate_vwap(bids, max_levels=2)
        # Top 2: 45¢ x 200, 42¢ x 50
        # VWAP = (45*200 + 42*50) / (200+50) = 11100/250 = 44.4¢
        44.4
    """
    if not bids:
        return None
    
    # Take top N levels (from the end since sorted low→high)
    top_levels = bids[-max_levels:] if len(bids) >= max_levels else bids
    
    total_value = sum(price * qty for price, qty in top_levels)
    total_qty = sum(qty for _, qty in top_levels)
    
    if total_qty == 0:
        return None
    
    return total_value / total_qty


def calculate_mid_price(pricing: MarketPricing) -> Optional[float]:
    """
    Calculate mid-price for YES side.
    
    Mid = (Best_Bid + Best_Ask) / 2
    
    Args:
        pricing: MarketPricing with bid/ask data
        
    Returns:
        Mid-price in cents, or None if insufficient data
    """
    if pricing.best_yes_bid is None or pricing.best_yes_ask is None:
        return None
    
    return (pricing.best_yes_bid + pricing.best_yes_ask) / 2


def analyze_orderbook(
    yes_bids: List[List[int]],
    no_bids: List[List[int]],
    depth_cents: int = 5
) -> dict:
    """
    Comprehensive orderbook analysis.
    
    Returns a dictionary with all key metrics for a market.
    
    Args:
        yes_bids: YES side bids
        no_bids: NO side bids
        depth_cents: Cents from best bid for depth calculation
        
    Returns:
        Dictionary with pricing, depth, and derived metrics
    """
    pricing = extract_best_prices(yes_bids, no_bids)
    
    if pricing is None:
        return {"error": "Insufficient orderbook data"}
    
    # Calculate additional metrics
    yes_depth_near = calculate_depth_at_price(yes_bids, depth_cents)
    no_depth_near = calculate_depth_at_price(no_bids, depth_cents)
    
    yes_vwap = calculate_vwap(yes_bids)
    no_vwap = calculate_vwap(no_bids)
    
    mid_price = calculate_mid_price(pricing)
    
    # Imbalance ratio: positive = more YES pressure, negative = more NO
    total_depth = pricing.yes_bid_depth + pricing.no_bid_depth
    imbalance = 0.0
    if total_depth > 0:
        imbalance = (pricing.yes_bid_depth - pricing.no_bid_depth) / total_depth
    
    return {
        # Core pricing
        "best_yes_bid": pricing.best_yes_bid,
        "best_yes_ask": pricing.best_yes_ask,
        "best_no_bid": pricing.best_no_bid,
        "best_no_ask": pricing.best_no_ask,
        "yes_spread": pricing.yes_spread,
        "no_spread": pricing.no_spread,
        
        # Depth metrics
        "yes_total_depth": pricing.yes_bid_depth,
        "no_total_depth": pricing.no_bid_depth,
        f"yes_depth_{depth_cents}c": yes_depth_near,
        f"no_depth_{depth_cents}c": no_depth_near,
        "yes_levels": len(yes_bids),
        "no_levels": len(no_bids),
        
        # Derived metrics
        "mid_price": mid_price,
        "yes_vwap": yes_vwap,
        "no_vwap": no_vwap,
        "imbalance": imbalance,  # -1 to +1 scale
    }


def format_orderbook_display(
    yes_bids: List[List[int]],
    no_bids: List[List[int]],
    levels: int = 5
) -> str:
    """
    Format orderbook for display (debugging/logging).
    
    Args:
        yes_bids: YES side bids
        no_bids: NO side bids
        levels: Number of levels to show from each side
        
    Returns:
        Formatted string representation
    """
    lines = []
    lines.append("=" * 50)
    lines.append("         YES BIDS          |          NO BIDS")
    lines.append("    Price    Qty    Cum    |    Price    Qty    Cum")
    lines.append("-" * 50)
    
    # Get top N levels (reversed since we want best first)
    yes_top = list(reversed(yes_bids[-levels:])) if yes_bids else []
    no_top = list(reversed(no_bids[-levels:])) if no_bids else []
    
    # Calculate cumulative quantities
    yes_cum = 0
    no_cum = 0
    
    max_rows = max(len(yes_top), len(no_top))
    
    for i in range(max_rows):
        # YES side
        if i < len(yes_top):
            price, qty = yes_top[i]
            yes_cum += qty
            yes_str = f"    {price:>3}¢  {qty:>6}  {yes_cum:>6}"
        else:
            yes_str = " " * 24
        
        # NO side
        if i < len(no_top):
            price, qty = no_top[i]
            no_cum += qty
            no_str = f"    {price:>3}¢  {qty:>6}  {no_cum:>6}"
        else:
            no_str = ""
        
        lines.append(f"{yes_str} | {no_str}")
    
    lines.append("=" * 50)
    
    return "\n".join(lines)


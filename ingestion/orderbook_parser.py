"""
Orderbook parsing utilities for extracting bid/ask prices.
"""
from typing import Optional
from models.market_data import MarketPricing


def extract_orderbook_prices(
    yes_bids: Optional[list],
    no_bids: Optional[list],
    yes_asks: Optional[list] = None,
    no_asks: Optional[list] = None
) -> Optional[MarketPricing]:
    """
    Extract actual bid and ask prices from orderbook data.
    
    Args:
        yes_bids: List of [price, quantity] pairs for Yes bids, or None
        no_bids: List of [price, quantity] pairs for No bids, or None
        yes_asks: List of [price, quantity] pairs for Yes asks, or None
        no_asks: List of [price, quantity] pairs for No asks, or None
        
    Returns:
        MarketPricing object with best bid/ask prices and spreads, or None if insufficient data
    """
    # Extract best Yes bid (LAST element is highest price per API docs)
    # Orderbook entries are [price, quantity] pairs, sorted from lowest to highest
    # Best bid is the HIGHEST price, which is the LAST element in the array
    best_yes_bid = None
    if yes_bids and len(yes_bids) > 0:
        # Last element is the highest price (best bid)
        best_yes_bid = float(yes_bids[-1][0])  # Last element, first value is price
    
    # Extract best No bid (LAST element is highest price per API docs)
    best_no_bid = None
    if no_bids and len(no_bids) > 0:
        # Last element is the highest price (best bid)
        best_no_bid = float(no_bids[-1][0])  # Last element, first value is price
    
    # Extract best Yes ask (FIRST element is lowest ask price)
    # Asks are typically sorted from lowest to highest, so first is best
    best_yes_ask = None
    if yes_asks and len(yes_asks) > 0:
        # First element is the lowest price (best ask)
        best_yes_ask = float(yes_asks[0][0])  # First element, first value is price
    elif yes_bids and best_no_bid is not None:
        # Fallback to implied ask if no actual asks available
        # Implied Ask Rule: Yes_Ask = 100 - Best_No_Bid
        best_yes_ask = 100.0 - best_no_bid
    
    # Extract best No ask (FIRST element is lowest ask price)
    best_no_ask = None
    if no_asks and len(no_asks) > 0:
        # First element is the lowest price (best ask)
        best_no_ask = float(no_asks[0][0])  # First element, first value is price
    elif no_bids and best_yes_bid is not None:
        # Fallback to implied ask if no actual asks available
        # Implied Ask Rule: No_Ask = 100 - Best_Yes_Bid
        best_no_ask = 100.0 - best_yes_bid
    
    # Need at least bids to return data
    if best_yes_bid is None or best_no_bid is None:
        return None
    
    # Create MarketPricing object
    pricing = MarketPricing(
        best_yes_bid=best_yes_bid,
        best_yes_ask=best_yes_ask,
        best_no_bid=best_no_bid,
        best_no_ask=best_no_ask
    )
    
    # Calculate spreads
    pricing.calculate_spreads()
    
    return pricing


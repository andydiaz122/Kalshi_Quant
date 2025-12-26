"""
QETE Utilities Package

Common utilities shared across the trading engine.

Modules:
- auth: API authentication utilities
- orderbook: Orderbook parsing and analysis
"""

# Lazy imports to avoid circular dependencies
__all__ = [
    "load_private_key_pem",
    "create_authenticated_client",
    "extract_best_prices",
    "analyze_orderbook",
]


def __getattr__(name):
    """Lazy import module contents."""
    if name in ("load_private_key_pem", "create_authenticated_client"):
        from .auth import load_private_key_pem, create_authenticated_client
        return locals()[name]
    elif name in ("extract_best_prices", "analyze_orderbook"):
        from .orderbook import extract_best_prices, analyze_orderbook
        return locals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


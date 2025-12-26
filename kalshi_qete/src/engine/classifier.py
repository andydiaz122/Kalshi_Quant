"""
Event Classifier

Classifies Kalshi events to determine if they are mutually exclusive.

CRITICAL: Only mutually exclusive events can be used for structural arbitrage.
Independent events (like pardons) would result in catastrophic losses if treated
as mutually exclusive.

Classification Methods:
1. **Metadata Inspection (Gold Standard)**: Check the `mutually_exclusive` field
   in the event JSON response from Kalshi API.
   
2. **Keyword Heuristics (Fallback)**: If metadata unavailable, use title keywords.
   - Qualify: "Nominee", "Winner", "Next Pope", "President"
   - Disqualify: "Pardon", "Cabinet", "Rate", "Price", "By", "Approval"

Usage:
    from kalshi_qete.src.engine.classifier import EventClassifier
    
    classifier = EventClassifier()
    
    # Check single event
    if classifier.is_mutually_exclusive("KXNEWPOPE-70"):
        print("Safe for structural arb!")
    
    # Filter list of events
    safe_events = classifier.filter_mutually_exclusive(event_tickers)
"""

import json
import logging
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set

# Kalshi public API
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Classification of event type for trading."""
    MUTUALLY_EXCLUSIVE = "mutually_exclusive"  # Safe for structural arb
    INDEPENDENT = "independent"                 # DO NOT use for structural arb
    UNKNOWN = "unknown"                         # Needs manual review


@dataclass
class EventClassification:
    """
    Result of classifying an event.
    
    Attributes:
        event_ticker: Event identifier
        event_type: Classification result
        confidence: How confident we are (1.0 = API confirmed, 0.5 = heuristic)
        source: How we determined this (api_metadata, keyword_heuristic, manual)
        title: Event title
        category: Event category
        raw_metadata: Full event metadata from API
    """
    event_ticker: str
    event_type: EventType
    confidence: float
    source: str
    title: Optional[str] = None
    category: Optional[str] = None
    raw_metadata: Optional[dict] = None
    
    @property
    def is_safe_for_arb(self) -> bool:
        """True if event can be used for structural arbitrage."""
        return self.event_type == EventType.MUTUALLY_EXCLUSIVE
    
    def __str__(self) -> str:
        safe = "✓ SAFE" if self.is_safe_for_arb else "✗ UNSAFE"
        return f"{safe} {self.event_ticker}: {self.event_type.value} ({self.source})"


class EventClassifier:
    """
    Classifies events as mutually exclusive or independent.
    
    Uses Kalshi's API metadata as the gold standard, falling back to
    keyword heuristics if metadata is unavailable.
    
    Example:
        >>> classifier = EventClassifier()
        >>> 
        >>> # Check if event is safe for structural arb
        >>> result = classifier.classify("KXNEWPOPE-70")
        >>> print(result)  # ✓ SAFE KXNEWPOPE-70: mutually_exclusive (api_metadata)
        >>> 
        >>> result = classifier.classify("KXTRUMPPARDONS-29JAN21")
        >>> print(result)  # ✗ UNSAFE KXTRUMPPARDONS-29JAN21: independent (api_metadata)
    """
    
    # Keyword lists for heuristic classification
    QUALIFYING_KEYWORDS = [
        "nominee", "winner", "next pope", "president elect",
        "first to", "who will win", "who will be",
        "champion", "governor", "senator", "mayor",
        "super bowl", "world series", "nba finals"
    ]
    
    DISQUALIFYING_KEYWORDS = [
        "pardon", "cabinet", "rate", "price", "by ", "by?",
        "approval", "poll", "gdp", "inflation", "unemployment",
        "temperature", "rain", "weather", "how many", "total",
        "combined", "cumulative", "schedule", "rescheduled"
    ]
    
    def __init__(self, cache_ttl_seconds: int = 3600):
        """
        Initialize the classifier.
        
        Args:
            cache_ttl_seconds: How long to cache classifications (default: 1 hour)
        """
        self.cache_ttl = cache_ttl_seconds
        self._cache: Dict[str, tuple] = {}  # event_ticker -> (classification, timestamp)
    
    def _fetch_event_metadata(self, event_ticker: str) -> Optional[dict]:
        """
        Fetch raw event metadata from Kalshi API.
        
        Args:
            event_ticker: Event to fetch
            
        Returns:
            Event metadata dict or None if fetch failed
        """
        url = f"{BASE_URL}/events/{event_ticker}"
        
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode('utf-8'))
                return data.get('event', data)
        except urllib.error.HTTPError as e:
            logger.warning(f"HTTP {e.code} fetching event {event_ticker}")
            return None
        except Exception as e:
            logger.warning(f"Error fetching event {event_ticker}: {e}")
            return None
    
    def _classify_by_metadata(self, metadata: dict) -> Optional[EventClassification]:
        """
        Classify event using API metadata (Gold Standard).
        
        Args:
            metadata: Event metadata from API
            
        Returns:
            EventClassification if mutually_exclusive field exists, else None
        """
        event_ticker = metadata.get('event_ticker', 'unknown')
        
        # Check for the mutually_exclusive flag
        me_flag = metadata.get('mutually_exclusive')
        
        if me_flag is not None:
            event_type = EventType.MUTUALLY_EXCLUSIVE if me_flag else EventType.INDEPENDENT
            
            return EventClassification(
                event_ticker=event_ticker,
                event_type=event_type,
                confidence=1.0,  # API is authoritative
                source="api_metadata",
                title=metadata.get('title'),
                category=metadata.get('category'),
                raw_metadata=metadata
            )
        
        # Also check collateral_return_type (MECNET = Mutually Exclusive Collateral Netted)
        collateral_type = metadata.get('collateral_return_type', '')
        if 'MEC' in collateral_type.upper():
            return EventClassification(
                event_ticker=event_ticker,
                event_type=EventType.MUTUALLY_EXCLUSIVE,
                confidence=0.9,  # High confidence from collateral type
                source="collateral_type",
                title=metadata.get('title'),
                category=metadata.get('category'),
                raw_metadata=metadata
            )
        
        return None  # Metadata didn't have the flag
    
    def _classify_by_keywords(
        self, 
        event_ticker: str,
        title: Optional[str] = None,
        category: Optional[str] = None
    ) -> EventClassification:
        """
        Classify event using keyword heuristics (Fallback).
        
        Args:
            event_ticker: Event identifier
            title: Event title (if known)
            category: Event category (if known)
            
        Returns:
            EventClassification based on keyword matching
        """
        # Combine available text for matching
        text = f"{event_ticker} {title or ''} {category or ''}".lower()
        
        # Check disqualifying keywords first (safer to reject)
        for keyword in self.DISQUALIFYING_KEYWORDS:
            if keyword.lower() in text:
                return EventClassification(
                    event_ticker=event_ticker,
                    event_type=EventType.INDEPENDENT,
                    confidence=0.6,
                    source=f"keyword_disqualify:{keyword}",
                    title=title,
                    category=category
                )
        
        # Check qualifying keywords
        for keyword in self.QUALIFYING_KEYWORDS:
            if keyword.lower() in text:
                return EventClassification(
                    event_ticker=event_ticker,
                    event_type=EventType.MUTUALLY_EXCLUSIVE,
                    confidence=0.5,
                    source=f"keyword_qualify:{keyword}",
                    title=title,
                    category=category
                )
        
        # Unknown - requires manual review
        return EventClassification(
            event_ticker=event_ticker,
            event_type=EventType.UNKNOWN,
            confidence=0.0,
            source="no_match",
            title=title,
            category=category
        )
    
    def classify(self, event_ticker: str, use_cache: bool = True) -> EventClassification:
        """
        Classify an event as mutually exclusive or independent.
        
        Uses API metadata (Gold Standard) first, falls back to keyword heuristics.
        
        Args:
            event_ticker: Event to classify
            use_cache: Whether to use cached results (default: True)
            
        Returns:
            EventClassification with type and confidence
        """
        # Check cache
        if use_cache and event_ticker in self._cache:
            classification, timestamp = self._cache[event_ticker]
            if (datetime.now() - timestamp).total_seconds() < self.cache_ttl:
                return classification
        
        logger.debug(f"Classifying event: {event_ticker}")
        
        # Try API metadata first (Gold Standard)
        metadata = self._fetch_event_metadata(event_ticker)
        
        if metadata:
            classification = self._classify_by_metadata(metadata)
            if classification:
                self._cache[event_ticker] = (classification, datetime.now())
                return classification
            
            # Metadata didn't have the flag - use keywords with metadata context
            classification = self._classify_by_keywords(
                event_ticker,
                title=metadata.get('title'),
                category=metadata.get('category')
            )
            classification.raw_metadata = metadata
        else:
            # No metadata available - use keywords only
            classification = self._classify_by_keywords(event_ticker)
        
        self._cache[event_ticker] = (classification, datetime.now())
        return classification
    
    def is_mutually_exclusive(self, event_ticker: str) -> bool:
        """
        Quick check if event is mutually exclusive.
        
        Args:
            event_ticker: Event to check
            
        Returns:
            True if event is confirmed mutually exclusive
        """
        classification = self.classify(event_ticker)
        return classification.is_safe_for_arb
    
    def filter_mutually_exclusive(
        self, 
        event_tickers: List[str],
        min_confidence: float = 0.8
    ) -> List[str]:
        """
        Filter list of events to only mutually exclusive ones.
        
        Args:
            event_tickers: List of events to filter
            min_confidence: Minimum confidence threshold (default: 0.8)
            
        Returns:
            List of event tickers that are mutually exclusive
        """
        safe_events = []
        
        for event_ticker in event_tickers:
            classification = self.classify(event_ticker)
            
            if (classification.event_type == EventType.MUTUALLY_EXCLUSIVE and 
                classification.confidence >= min_confidence):
                safe_events.append(event_ticker)
                logger.info(f"✓ {event_ticker}: safe for arb ({classification.source})")
            else:
                logger.info(
                    f"✗ {event_ticker}: excluded "
                    f"({classification.event_type.value}, conf={classification.confidence})"
                )
        
        return safe_events
    
    def classify_batch(
        self, 
        event_tickers: List[str]
    ) -> Dict[str, EventClassification]:
        """
        Classify multiple events.
        
        Args:
            event_tickers: List of events to classify
            
        Returns:
            Dictionary mapping event_ticker to classification
        """
        results = {}
        for event_ticker in event_tickers:
            results[event_ticker] = self.classify(event_ticker)
        return results
    
    def get_safe_events_summary(
        self, 
        classifications: Dict[str, EventClassification]
    ) -> str:
        """
        Get a summary of classifications.
        
        Args:
            classifications: Dictionary of classifications
            
        Returns:
            Formatted summary string
        """
        lines = [
            "Event Classification Summary",
            "=" * 60
        ]
        
        safe = [c for c in classifications.values() if c.is_safe_for_arb]
        unsafe = [c for c in classifications.values() if c.event_type == EventType.INDEPENDENT]
        unknown = [c for c in classifications.values() if c.event_type == EventType.UNKNOWN]
        
        lines.append(f"\n✓ Safe for Structural Arb: {len(safe)}")
        for c in safe:
            lines.append(f"   {c.event_ticker} ({c.source}, conf={c.confidence:.1f})")
        
        lines.append(f"\n✗ Unsafe (Independent): {len(unsafe)}")
        for c in unsafe:
            lines.append(f"   {c.event_ticker} ({c.source})")
        
        if unknown:
            lines.append(f"\n⚠ Unknown (Manual Review): {len(unknown)}")
            for c in unknown:
                lines.append(f"   {c.event_ticker}")
        
        return "\n".join(lines)


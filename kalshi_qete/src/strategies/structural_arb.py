"""
Structural Arbitrage Strategy

Identifies risk-free mispricings in mutually exclusive Kalshi markets.

Theory:
    In a mutually exclusive event (e.g., "Republican Nominee", "Super Bowl Winner"),
    exactly ONE outcome will settle YES. The probabilities MUST sum to 100%.
    
    If market prices deviate from this constraint, arbitrage exists:
    
    BUY ARB (Sum < 1.00):
        - Buy YES on ALL outcomes at their ask prices
        - Guaranteed payout: $1.00 (one contract settles YES)
        - If Sum(Asks) < $0.98, profit ≥ $0.02 per $1.00 (after fees)
        
    SELL ARB (Sum > 1.00):
        - Sell YES on ALL outcomes at their bid prices
        - Collect premium upfront, pay $1.00 when one settles
        - If Sum(Bids) > $1.02, profit ≥ $0.02 per $1.00 (after fees)

Example:
    Event: "GOP Nominee 2024"
    Markets: Trump (45¢), DeSantis (30¢), Haley (20¢), Other (8¢)
    Sum(Asks) = 103¢ → No buy arb
    Sum(Bids) = 97¢ → No sell arb
    
    But if Sum(Asks) = 96¢:
        Buy all 4 at 96¢ total → Guaranteed 100¢ payout → 4¢ profit!

IMPORTANT: This strategy requires COMPLETE event data to avoid false positives.
Use fetch_complete_events=True (default) to automatically fetch all markets
for each event before analysis.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Tuple, Optional, TYPE_CHECKING

from kalshi_qete.src.strategies.base import Strategy, Signal, SignalGroup, Side
from kalshi_qete.src.engine.scanner import MarketWithOrderbook

if TYPE_CHECKING:
    from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter

logger = logging.getLogger(__name__)


@dataclass
class EventAnalysis:
    """
    Analysis of a mutually exclusive event for arbitrage opportunities.
    
    Attributes:
        event_ticker: Event identifier
        markets: List of markets in this event
        sum_yes_asks: Total cost to buy YES on all outcomes
        sum_yes_bids: Total premium from selling YES on all outcomes
        buy_arb_profit: Profit from buy arb (negative if no opportunity)
        sell_arb_profit: Profit from sell arb (negative if no opportunity)
        market_count: Number of markets in event
        total_markets: Total markets in event (for coverage calculation)
        aggregate_volume: Total 24h contract volume across all markets
    """
    event_ticker: str
    markets: List[MarketWithOrderbook]
    sum_yes_asks: float  # In cents
    sum_yes_bids: float  # In cents
    buy_arb_profit: float  # In cents (100 - sum_asks - fees)
    sell_arb_profit: float  # In cents (sum_bids - 100 - fees)
    market_count: int
    timestamp: datetime
    total_markets: int = 0  # For coverage calculation
    aggregate_volume: int = 0  # Total 24h contract volume
    
    @property
    def coverage(self) -> float:
        """Percentage of markets with valid pricing."""
        if self.total_markets == 0:
            return 0.0
        return self.market_count / self.total_markets
    
    @property
    def has_buy_arb(self) -> bool:
        """True if profitable buy arbitrage exists."""
        return self.buy_arb_profit > 0
    
    @property
    def has_sell_arb(self) -> bool:
        """True if profitable sell arbitrage exists."""
        return self.sell_arb_profit > 0
    
    @property
    def has_opportunity(self) -> bool:
        """True if any arbitrage opportunity exists."""
        return self.has_buy_arb or self.has_sell_arb
    
    def is_high_quality(
        self, 
        min_coverage: float = 0.9, 
        min_contracts: int = 10000
    ) -> bool:
        """
        Check if opportunity meets quality thresholds.
        
        Args:
            min_coverage: Minimum coverage ratio (default: 90%)
            min_contracts: Minimum aggregate 24h contract volume (default: 10,000)
            
        Returns:
            True if opportunity is high quality
        """
        if not self.has_opportunity:
            return False
        if self.coverage < min_coverage:
            return False
        if self.aggregate_volume < min_contracts:
            return False
        return True
    
    def __str__(self) -> str:
        status = "ARB!" if self.has_opportunity else "No Arb"
        return (
            f"EventAnalysis({self.event_ticker}): {self.market_count} markets, "
            f"Sum(Asks)={self.sum_yes_asks:.1f}¢, Sum(Bids)={self.sum_yes_bids:.1f}¢, "
            f"Cov={self.coverage*100:.0f}%, Contracts={self.aggregate_volume:,} "
            f"[{status}]"
        )


class StructuralArbStrategy(Strategy):
    """
    Structural Arbitrage Strategy for mutually exclusive events.
    
    Scans events where exactly one outcome must settle YES and looks
    for mispricings where probability sums deviate from 100%.
    
    Parameters:
        buy_threshold: Maximum sum of asks to trigger BUY ALL (default: 98¢)
        sell_threshold: Minimum sum of bids to trigger SELL ALL (default: 102¢)
        min_markets: Minimum markets in event to consider (default: 2)
        default_size: Default position size per contract (default: 10)
    
    Example:
        >>> strategy = StructuralArbStrategy(buy_threshold=98, sell_threshold=102)
        >>> signals = strategy.generate_signals(markets)
    """
    
    def __init__(
        self,
        buy_threshold: float = 98.0,   # Sum < 98¢ triggers buy
        sell_threshold: float = 102.0,  # Sum > 102¢ triggers sell
        min_markets: int = 2,           # Need at least 2 markets
        max_markets: int = 50,          # Skip events with too many markets
        default_size: int = 10,         # Contracts per signal
        require_two_sided: bool = True  # Only use two-sided markets
    ):
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.min_markets = min_markets
        self.max_markets = max_markets
        self.default_size = default_size
        self.require_two_sided = require_two_sided
        
        # Track analysis results
        self.last_analysis: Dict[str, EventAnalysis] = {}
    
    @property
    def name(self) -> str:
        return "StructuralArb"
    
    def _group_by_event(
        self, 
        markets: List[MarketWithOrderbook]
    ) -> Dict[str, List[MarketWithOrderbook]]:
        """
        Group markets by their event_ticker.
        
        Args:
            markets: List of markets with orderbook data
            
        Returns:
            Dictionary mapping event_ticker to list of markets
        """
        groups = defaultdict(list)
        
        for market in markets:
            event_ticker = market.market.event_ticker
            if event_ticker:
                groups[event_ticker].append(market)
        
        return dict(groups)
    
    def _analyze_event(
        self,
        event_ticker: str,
        markets: List[MarketWithOrderbook]
    ) -> EventAnalysis:
        """
        Analyze a single event for arbitrage opportunities.
        
        Args:
            event_ticker: Event identifier
            markets: All markets in this event
            
        Returns:
            EventAnalysis with sum calculations and profit potential
        """
        sum_yes_asks = 0.0
        sum_yes_bids = 0.0
        valid_markets = []
        aggregate_volume = 0
        
        for market in markets:
            # Accumulate volume from all markets (even without pricing)
            aggregate_volume += getattr(market.market, 'volume_24h', 0) or 0
            
            pricing = market.pricing
            
            # Skip markets without valid pricing
            if pricing is None:
                continue
            
            # Get YES ask and bid
            yes_ask = pricing.best_yes_ask
            yes_bid = pricing.best_yes_bid
            
            # Skip if no ask (can't buy)
            if yes_ask is None or yes_ask <= 0:
                continue
            
            # For two-sided requirement, skip if no bid
            if self.require_two_sided and (yes_bid is None or yes_bid <= 0):
                continue
            
            sum_yes_asks += yes_ask
            sum_yes_bids += yes_bid if yes_bid else 0
            valid_markets.append(market)
        
        # Calculate profit potential
        # Fee assumption: ~2% total (1% buy + 1% settlement)
        # Buy arb profit = 100 - sum_asks (guaranteed payout minus cost)
        # Sell arb profit = sum_bids - 100 (premium minus guaranteed payout)
        buy_arb_profit = 100 - sum_yes_asks if sum_yes_asks > 0 else -100
        sell_arb_profit = sum_yes_bids - 100 if sum_yes_bids > 0 else -100
        
        return EventAnalysis(
            event_ticker=event_ticker,
            markets=valid_markets,
            sum_yes_asks=sum_yes_asks,
            sum_yes_bids=sum_yes_bids,
            buy_arb_profit=buy_arb_profit,
            sell_arb_profit=sell_arb_profit,
            market_count=len(valid_markets),
            timestamp=datetime.now(),
            total_markets=len(markets),  # All markets (for coverage)
            aggregate_volume=aggregate_volume
        )
    
    def analyze_all_events(
        self,
        markets: List[MarketWithOrderbook]
    ) -> List[EventAnalysis]:
        """
        Analyze all events in the market data.
        
        Args:
            markets: List of markets with orderbook data
            
        Returns:
            List of EventAnalysis for each event
        """
        groups = self._group_by_event(markets)
        analyses = []
        
        for event_ticker, event_markets in groups.items():
            # Skip events with too few or too many markets
            if len(event_markets) < self.min_markets:
                continue
            if len(event_markets) > self.max_markets:
                continue
            
            analysis = self._analyze_event(event_ticker, event_markets)
            analyses.append(analysis)
            
            # Store for later reference
            self.last_analysis[event_ticker] = analysis
        
        return analyses
    
    def generate_signals(
        self,
        markets: List[MarketWithOrderbook]
    ) -> List[Signal]:
        """
        Generate arbitrage signals for all markets.
        
        Analyzes markets grouped by event and generates BUY ALL or SELL ALL
        signals when arbitrage opportunities exist.
        
        Args:
            markets: List of markets with orderbook data
            
        Returns:
            List of Signal objects for execution
        """
        signals = []
        analyses = self.analyze_all_events(markets)
        
        for analysis in analyses:
            # Check for buy arbitrage
            if analysis.sum_yes_asks < self.buy_threshold and analysis.sum_yes_asks > 0:
                for market in analysis.markets:
                    if market.pricing and market.pricing.best_yes_ask:
                        signals.append(Signal(
                            ticker=market.market.ticker,
                            side=Side.BUY,
                            price=int(market.pricing.best_yes_ask),
                            size=self.default_size,
                            strategy_name=self.name,
                            confidence=min(1.0, analysis.buy_arb_profit / 5),  # Scale by profit
                            metadata={
                                "event_ticker": analysis.event_ticker,
                                "arb_type": "BUY_ALL",
                                "sum_asks": analysis.sum_yes_asks,
                                "expected_profit_cents": analysis.buy_arb_profit,
                                "market_count": analysis.market_count
                            }
                        ))
            
            # Check for sell arbitrage
            if analysis.sum_yes_bids > self.sell_threshold:
                for market in analysis.markets:
                    if market.pricing and market.pricing.best_yes_bid:
                        signals.append(Signal(
                            ticker=market.market.ticker,
                            side=Side.SELL,
                            price=int(market.pricing.best_yes_bid),
                            size=self.default_size,
                            strategy_name=self.name,
                            confidence=min(1.0, analysis.sell_arb_profit / 5),
                            metadata={
                                "event_ticker": analysis.event_ticker,
                                "arb_type": "SELL_ALL",
                                "sum_bids": analysis.sum_yes_bids,
                                "expected_profit_cents": analysis.sell_arb_profit,
                                "market_count": analysis.market_count
                            }
                        ))
        
        return signals
    
    def generate_signal_groups(
        self,
        markets: List[MarketWithOrderbook]
    ) -> List[SignalGroup]:
        """
        Generate grouped signals for multi-leg arbitrage.
        
        Each SignalGroup represents a complete arbitrage package
        (all legs of a BUY ALL or SELL ALL trade).
        
        Args:
            markets: List of markets with orderbook data
            
        Returns:
            List of SignalGroup objects
        """
        signal_groups = []
        analyses = self.analyze_all_events(markets)
        
        for analysis in analyses:
            # Buy arbitrage group
            if analysis.sum_yes_asks < self.buy_threshold and analysis.sum_yes_asks > 0:
                buy_signals = []
                for market in analysis.markets:
                    if market.pricing and market.pricing.best_yes_ask:
                        buy_signals.append(Signal(
                            ticker=market.market.ticker,
                            side=Side.BUY,
                            price=int(market.pricing.best_yes_ask),
                            size=self.default_size,
                            strategy_name=self.name,
                            metadata={"event_ticker": analysis.event_ticker}
                        ))
                
                if buy_signals:
                    expected_profit = (100 - analysis.sum_yes_asks) * self.default_size / 100
                    signal_groups.append(SignalGroup(
                        signals=buy_signals,
                        group_name=f"BUY_ALL_{analysis.event_ticker}",
                        event_ticker=analysis.event_ticker,
                        expected_profit=expected_profit,
                        strategy_name=self.name,
                        metadata={
                            "arb_type": "BUY_ALL",
                            "sum_asks": analysis.sum_yes_asks,
                            "profit_per_dollar": 100 - analysis.sum_yes_asks
                        }
                    ))
            
            # Sell arbitrage group
            if analysis.sum_yes_bids > self.sell_threshold:
                sell_signals = []
                for market in analysis.markets:
                    if market.pricing and market.pricing.best_yes_bid:
                        sell_signals.append(Signal(
                            ticker=market.market.ticker,
                            side=Side.SELL,
                            price=int(market.pricing.best_yes_bid),
                            size=self.default_size,
                            strategy_name=self.name,
                            metadata={"event_ticker": analysis.event_ticker}
                        ))
                
                if sell_signals:
                    expected_profit = (analysis.sum_yes_bids - 100) * self.default_size / 100
                    signal_groups.append(SignalGroup(
                        signals=sell_signals,
                        group_name=f"SELL_ALL_{analysis.event_ticker}",
                        event_ticker=analysis.event_ticker,
                        expected_profit=expected_profit,
                        strategy_name=self.name,
                        metadata={
                            "arb_type": "SELL_ALL",
                            "sum_bids": analysis.sum_yes_bids,
                            "profit_per_dollar": analysis.sum_yes_bids - 100
                        }
                    ))
        
        return signal_groups
    
    def get_summary(self) -> str:
        """
        Get a summary of the last analysis run.
        
        Returns:
            Formatted string with analysis summary
        """
        if not self.last_analysis:
            return "No analysis run yet"
        
        lines = [
            f"StructuralArb Analysis Summary ({len(self.last_analysis)} events)",
            "=" * 60
        ]
        
        opportunities = []
        close_calls = []
        
        for event_ticker, analysis in sorted(self.last_analysis.items()):
            if analysis.has_opportunity:
                opportunities.append(analysis)
            elif analysis.sum_yes_asks < 105 or analysis.sum_yes_bids > 95:
                close_calls.append(analysis)
        
        if opportunities:
            lines.append(f"\n🎯 ARBITRAGE OPPORTUNITIES ({len(opportunities)}):")
            for a in opportunities:
                if a.has_buy_arb:
                    lines.append(
                        f"  BUY {a.event_ticker}: Sum(Asks)={a.sum_yes_asks:.1f}¢, "
                        f"Profit={a.buy_arb_profit:.1f}¢/contract"
                    )
                if a.has_sell_arb:
                    lines.append(
                        f"  SELL {a.event_ticker}: Sum(Bids)={a.sum_yes_bids:.1f}¢, "
                        f"Profit={a.sell_arb_profit:.1f}¢/contract"
                    )
        else:
            lines.append("\n❌ No arbitrage opportunities found")
        
        if close_calls:
            lines.append(f"\n👀 CLOSE TO ARB ({len(close_calls)}):")
            for a in sorted(close_calls, key=lambda x: x.sum_yes_asks)[:10]:
                lines.append(
                    f"  {a.event_ticker}: Asks={a.sum_yes_asks:.1f}¢, "
                    f"Bids={a.sum_yes_bids:.1f}¢ ({a.market_count} mkts)"
                )
        
        return "\n".join(lines)


@dataclass
class CompleteEventData:
    """
    Complete event data with all markets and orderbooks.
    
    This is the result of fetching ALL markets for an event,
    not just the ones that appeared in top volume scan.
    """
    event_ticker: str
    total_markets: int
    markets_with_orderbook: int
    markets_with_pricing: int
    markets: List[MarketWithOrderbook]
    source_market_count: int  # How many markets we saw in initial scan
    
    @property
    def completeness(self) -> float:
        """Ratio of markets with pricing to total markets."""
        if self.total_markets == 0:
            return 0.0
        return self.markets_with_pricing / self.total_markets


class StructuralArbScanner:
    """
    Complete Structural Arbitrage Scanner.
    
    Unlike the basic StructuralArbStrategy which only analyzes markets
    passed to it, this scanner:
    
    1. Identifies events from top-volume markets
    2. **CLASSIFIES events** to filter out non-mutually-exclusive ones
    3. Fetches ALL markets for each SAFE event (eliminates false positives)
    4. Gets orderbook data for complete coverage
    5. Runs arbitrage analysis on complete data
    
    CRITICAL: Only events with mutually_exclusive=True are analyzed.
    This prevents catastrophic losses from treating independent events as ME.
    
    Example:
        >>> from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter
        >>> from kalshi_qete.src.strategies.structural_arb import StructuralArbScanner
        >>> 
        >>> adapter = KalshiAdapter(key_id, key_path)
        >>> scanner = StructuralArbScanner(adapter)
        >>> 
        >>> # Scan top 100 volume markets, fetch complete events
        >>> analyses = scanner.scan_top_volume(n=100)
        >>> 
        >>> # Get opportunities (ONLY from mutually exclusive events)
        >>> for a in analyses:
        ...     if a.has_opportunity:
        ...         print(f"REAL ARB: {a}")
    """
    
    def __init__(
        self,
        adapter: "KalshiAdapter",
        buy_threshold: float = 98.0,
        sell_threshold: float = 102.0,
        min_markets: int = 2,
        max_markets: int = 50,
        default_size: int = 10,
        require_mutually_exclusive: bool = True
    ):
        """
        Initialize the complete scanner.
        
        Args:
            adapter: KalshiAdapter for API calls
            buy_threshold: Sum of asks below this triggers buy signal (default: 98¢)
            sell_threshold: Sum of bids above this triggers sell signal (default: 102¢)
            min_markets: Minimum markets in event to consider (default: 2)
            max_markets: Maximum markets in event (skip huge events) (default: 50)
            default_size: Default position size per contract (default: 10)
            require_mutually_exclusive: Only analyze ME events (default: True, HIGHLY RECOMMENDED)
        """
        self.adapter = adapter
        self.require_mutually_exclusive = require_mutually_exclusive
        
        self.strategy = StructuralArbStrategy(
            buy_threshold=buy_threshold,
            sell_threshold=sell_threshold,
            min_markets=min_markets,
            max_markets=max_markets,
            default_size=default_size,
            require_two_sided=True
        )
        
        # Import classifier
        from kalshi_qete.src.engine.classifier import EventClassifier
        self.classifier = EventClassifier()
        
        # Track complete event data
        self.complete_events: Dict[str, CompleteEventData] = {}
        self.excluded_events: Dict[str, str] = {}  # event_ticker -> reason
        self.last_analyses: List[EventAnalysis] = []
    
    def _extract_event_tickers(
        self, 
        markets: List[MarketWithOrderbook]
    ) -> List[str]:
        """
        Extract unique event tickers from markets.
        
        Args:
            markets: List of markets with orderbook data
            
        Returns:
            List of unique event tickers
        """
        event_tickers = set()
        for market in markets:
            if market.market.event_ticker:
                event_tickers.add(market.market.event_ticker)
        return list(event_tickers)
    
    def _fetch_complete_event(
        self,
        event_ticker: str,
        source_market_count: int = 0
    ) -> CompleteEventData:
        """
        Fetch ALL markets for an event with orderbook data.
        
        Args:
            event_ticker: Event to fetch completely
            source_market_count: How many markets we saw in initial scan
            
        Returns:
            CompleteEventData with all markets
        """
        logger.info(f"Fetching complete data for event: {event_ticker}")
        
        # Get all markets in this event
        all_market_infos = self.adapter.get_markets_by_event(event_ticker)
        
        logger.debug(f"  Found {len(all_market_infos)} total markets")
        
        # Get orderbook for each market
        markets_with_orderbook = []
        for market_info in all_market_infos:
            try:
                orderbook, pricing = self.adapter.get_orderbook_with_pricing(
                    market_info.ticker
                )
                
                markets_with_orderbook.append(MarketWithOrderbook(
                    market=market_info,
                    orderbook=orderbook,
                    pricing=pricing,
                    analysis=None
                ))
            except Exception as e:
                logger.warning(f"  Failed to get orderbook for {market_info.ticker}: {e}")
                # Still include market but without orderbook
                markets_with_orderbook.append(MarketWithOrderbook(
                    market=market_info,
                    orderbook=None,
                    pricing=None,
                    analysis=None
                ))
        
        # Count markets with valid pricing
        markets_with_pricing = len([
            m for m in markets_with_orderbook 
            if m.pricing and m.pricing.best_yes_ask
        ])
        
        return CompleteEventData(
            event_ticker=event_ticker,
            total_markets=len(all_market_infos),
            markets_with_orderbook=len([m for m in markets_with_orderbook if m.orderbook]),
            markets_with_pricing=markets_with_pricing,
            markets=markets_with_orderbook,
            source_market_count=source_market_count
        )
    
    def scan_top_volume(
        self,
        n: int = 100,
        min_volume: int = 100
    ) -> List[EventAnalysis]:
        """
        Scan top volume markets with COMPLETE event data.
        
        This is the main method for finding real arbitrage:
        1. Gets top N markets by volume
        2. Extracts unique events from those markets
        3. **CLASSIFIES events** - filters to mutually exclusive only
        4. Fetches ALL markets for each SAFE event
        5. Analyzes complete data
        
        CRITICAL: Only mutually exclusive events are analyzed to prevent
        catastrophic losses from misclassified independent events.
        
        Args:
            n: Number of top markets to scan for event discovery
            min_volume: Minimum volume filter
            
        Returns:
            List of EventAnalysis with complete data (only from ME events)
        """
        from kalshi_qete.src.engine.scanner import MarketScanner
        
        logger.info(f"Starting complete scan: top {n} markets, min_volume={min_volume}")
        
        # Step 1: Get top volume markets
        scanner = MarketScanner(self.adapter)
        top_markets = scanner.scan_top_volume(n=n, min_volume=min_volume)
        
        logger.info(f"Step 1: Found {len(top_markets)} top volume markets")
        
        # Step 2: Extract unique events
        event_tickers = self._extract_event_tickers(top_markets)
        logger.info(f"Step 2: Identified {len(event_tickers)} unique events")
        
        # Count markets per event from initial scan
        event_counts = {}
        for market in top_markets:
            et = market.market.event_ticker
            if et:
                event_counts[et] = event_counts.get(et, 0) + 1
        
        # Step 3: CLASSIFY events - filter to mutually exclusive only
        safe_events = []
        self.excluded_events = {}
        
        if self.require_mutually_exclusive:
            logger.info("Step 3: Classifying events (filtering to mutually exclusive)...")
            
            for event_ticker in event_tickers:
                classification = self.classifier.classify(event_ticker)
                
                if classification.is_safe_for_arb:
                    safe_events.append(event_ticker)
                    logger.info(f"  ✓ {event_ticker}: mutually_exclusive ({classification.source})")
                else:
                    self.excluded_events[event_ticker] = (
                        f"{classification.event_type.value} ({classification.source})"
                    )
                    logger.info(f"  ✗ {event_ticker}: EXCLUDED - {classification.event_type.value}")
            
            logger.info(
                f"Step 3: {len(safe_events)}/{len(event_tickers)} events are mutually exclusive"
            )
        else:
            # WARNING: Dangerous mode - analyze all events
            logger.warning("⚠️ DANGER: require_mutually_exclusive=False - analyzing ALL events!")
            safe_events = event_tickers
        
        # Step 4: Fetch complete data for each SAFE event
        all_complete_markets = []
        for event_ticker in safe_events:
            source_count = event_counts.get(event_ticker, 0)
            
            complete_event = self._fetch_complete_event(
                event_ticker, 
                source_market_count=source_count
            )
            
            # Store for reference
            self.complete_events[event_ticker] = complete_event
            
            # Log if we found more markets than in initial scan
            if complete_event.total_markets > source_count:
                logger.info(
                    f"  {event_ticker}: {source_count} → {complete_event.total_markets} markets "
                    f"(+{complete_event.total_markets - source_count} discovered)"
                )
            
            all_complete_markets.extend(complete_event.markets)
        
        logger.info(f"Step 4: Fetched {len(all_complete_markets)} total markets from safe events")
        
        # Step 5: Run arbitrage analysis
        self.last_analyses = self.strategy.analyze_all_events(all_complete_markets)
        
        logger.info(f"Step 5: Analyzed {len(self.last_analyses)} mutually exclusive events")
        
        return self.last_analyses
    
    def get_excluded_events_summary(self) -> str:
        """Get summary of events excluded from analysis."""
        if not self.excluded_events:
            return "No events excluded"
        
        lines = [
            f"Excluded Events ({len(self.excluded_events)}):",
            "-" * 40
        ]
        for event_ticker, reason in sorted(self.excluded_events.items()):
            lines.append(f"  ✗ {event_ticker}: {reason}")
        
        return "\n".join(lines)
    
    def scan_events(
        self,
        event_tickers: List[str]
    ) -> List[EventAnalysis]:
        """
        Scan specific events with complete data.
        
        Args:
            event_tickers: List of events to analyze
            
        Returns:
            List of EventAnalysis
        """
        logger.info(f"Scanning {len(event_tickers)} specific events")
        
        all_markets = []
        for event_ticker in event_tickers:
            complete_event = self._fetch_complete_event(event_ticker)
            self.complete_events[event_ticker] = complete_event
            all_markets.extend(complete_event.markets)
        
        self.last_analyses = self.strategy.analyze_all_events(all_markets)
        return self.last_analyses
    
    def get_opportunities(self) -> List[EventAnalysis]:
        """
        Get events with arbitrage opportunities from last scan.
        
        Returns:
            List of EventAnalysis where has_opportunity is True
        """
        return [a for a in self.last_analyses if a.has_opportunity]
    
    def get_high_quality_opportunities(
        self,
        min_coverage: float = 0.9,
        min_contracts: int = 10000
    ) -> List[EventAnalysis]:
        """
        Get only high-quality arbitrage opportunities.
        
        Filters by:
        - Coverage >= min_coverage (default: 90%)
        - Aggregate contract volume >= min_contracts (default: 10,000)
        
        This eliminates:
        - Low coverage events (incomplete data, risky)
        - Low volume events (capital traps, illiquid)
        
        Args:
            min_coverage: Minimum coverage ratio
            min_contracts: Minimum aggregate 24h contract volume
            
        Returns:
            List of high-quality EventAnalysis
        """
        return [
            a for a in self.last_analyses 
            if a.is_high_quality(min_coverage, min_contracts)
        ]
    
    def get_filtered_signal_groups(
        self,
        min_coverage: float = 0.9,
        min_contracts: int = 10000
    ) -> List[SignalGroup]:
        """
        Get signal groups only for high-quality opportunities.
        
        Args:
            min_coverage: Minimum coverage ratio
            min_contracts: Minimum aggregate 24h contract volume
            
        Returns:
            List of SignalGroup for execution
        """
        hq_events = {
            a.event_ticker 
            for a in self.get_high_quality_opportunities(min_coverage, min_contracts)
        }
        
        all_groups = self.get_signal_groups()
        
        return [g for g in all_groups if g.event_ticker in hq_events]
    
    def get_signals(self) -> List[Signal]:
        """
        Generate trading signals from last scan.
        
        Returns:
            List of Signal objects for execution
        """
        all_markets = []
        for complete_event in self.complete_events.values():
            all_markets.extend(complete_event.markets)
        
        return self.strategy.generate_signals(all_markets)
    
    def get_signal_groups(self) -> List[SignalGroup]:
        """
        Generate grouped signals from last scan.
        
        Returns:
            List of SignalGroup objects
        """
        all_markets = []
        for complete_event in self.complete_events.values():
            all_markets.extend(complete_event.markets)
        
        return self.strategy.generate_signal_groups(all_markets)
    
    def get_completeness_report(self) -> str:
        """
        Get a report on data completeness for each event.
        
        Returns:
            Formatted string showing coverage statistics
        """
        lines = [
            "Event Data Completeness Report",
            "=" * 60,
            ""
        ]
        
        for event_ticker, data in sorted(self.complete_events.items()):
            coverage = data.completeness * 100
            discovered = data.total_markets - data.source_market_count
            
            status = "✓" if coverage >= 80 else "⚠️" if coverage >= 50 else "❌"
            
            lines.append(
                f"{status} {event_ticker}: "
                f"{data.markets_with_pricing}/{data.total_markets} markets with pricing "
                f"({coverage:.0f}% coverage)"
            )
            
            if discovered > 0:
                lines.append(f"    → Discovered {discovered} additional markets")
        
        return "\n".join(lines)


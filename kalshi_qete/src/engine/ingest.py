"""
Data Ingestion Pipeline

Orchestrates the flow of market data from Kalshi API to DuckDB storage.

Pipeline Flow:
    Kalshi API → Adapter → Scanner → Orderbook Parser → DuckDB Store

Supports multiple ingestion modes:
- Single event scan
- Series scan
- Full market scan
- Continuous polling

Usage:
    from kalshi_qete.src.engine.ingest import IngestionPipeline
    
    pipeline = IngestionPipeline()
    pipeline.ingest_event("KXFEDCHAIRNOM-29")
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Callable

from kalshi_qete import config
from kalshi_qete.src.adapters.kalshi_adapter import KalshiAdapter
from kalshi_qete.src.db.duckdb_store import DuckDBStore
from kalshi_qete.src.db.models import OrderbookSnapshot
from kalshi_qete.src.engine.scanner import MarketScanner, MarketWithOrderbook


# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """
    Result of an ingestion run.
    
    Provides summary statistics and any errors encountered.
    """
    success: bool
    markets_scanned: int
    snapshots_stored: int
    errors: List[str]
    duration_seconds: float
    timestamp: datetime
    
    def __str__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"IngestionResult({status}): "
            f"{self.snapshots_stored}/{self.markets_scanned} markets stored "
            f"in {self.duration_seconds:.2f}s"
        )


class IngestionPipeline:
    """
    Orchestrates data ingestion from Kalshi API to DuckDB.
    
    Provides methods for different ingestion strategies:
    - Event-based: Ingest all markets in a specific event
    - Series-based: Ingest all markets in a series
    - Top volume: Ingest the most active markets
    - Continuous: Poll at intervals
    
    Example:
        >>> pipeline = IngestionPipeline()
        >>> result = pipeline.ingest_event("KXFEDCHAIRNOM-29")
        >>> print(result)
        IngestionResult(SUCCESS): 7/23 markets stored in 2.45s
    """
    
    def __init__(
        self,
        db_path: Optional[Path] = None,
        key_id: Optional[str] = None,
        key_file_path: Optional[Path] = None
    ):
        """
        Initialize the ingestion pipeline.
        
        Args:
            db_path: Path to DuckDB database (default: from config)
            key_id: Kalshi API key ID (default: from config)
            key_file_path: Path to API key file (default: from config)
        """
        # Use config defaults if not provided
        self.db_path = db_path or config.DB_PATH
        self.key_id = key_id or config.KEY_ID
        self.key_file_path = key_file_path or config.KEY_FILE_PATH
        
        # Initialize components (lazy - created on first use)
        self._adapter: Optional[KalshiAdapter] = None
        self._scanner: Optional[MarketScanner] = None
        self._store: Optional[DuckDBStore] = None
        
        logger.info(f"IngestionPipeline initialized (db: {self.db_path})")
    
    @property
    def adapter(self) -> KalshiAdapter:
        """Lazy-load the Kalshi adapter."""
        if self._adapter is None:
            logger.debug("Creating KalshiAdapter...")
            self._adapter = KalshiAdapter(self.key_id, self.key_file_path)
        return self._adapter
    
    @property
    def scanner(self) -> MarketScanner:
        """Lazy-load the market scanner."""
        if self._scanner is None:
            logger.debug("Creating MarketScanner...")
            self._scanner = MarketScanner(self.adapter)
        return self._scanner
    
    @property
    def store(self) -> DuckDBStore:
        """Lazy-load the DuckDB store."""
        if self._store is None:
            logger.debug(f"Opening DuckDB at {self.db_path}...")
            self._store = DuckDBStore(self.db_path)
        return self._store
    
    # =========================================================================
    # INGESTION METHODS
    # =========================================================================
    
    def ingest_event(
        self,
        event_ticker: str,
        min_volume: int = 0,
        two_sided_only: bool = False
    ) -> IngestionResult:
        """
        Ingest all markets in an event.
        
        Args:
            event_ticker: Event to ingest (e.g., "KXFEDCHAIRNOM-29")
            min_volume: Minimum 24h volume filter
            two_sided_only: Only ingest markets with both YES and NO bids
            
        Returns:
            IngestionResult with summary statistics
        """
        start_time = time.time()
        errors = []
        
        logger.info(f"Starting ingestion for event: {event_ticker}")
        
        try:
            # Scan event
            markets = self.scanner.scan_event(event_ticker, min_volume=min_volume)
            logger.info(f"Scanned {len(markets)} markets")
            
            # Apply two-sided filter if requested
            if two_sided_only:
                markets = self.scanner.filter_by_two_sided(markets)
                logger.info(f"Filtered to {len(markets)} two-sided markets")
            
            # Create and store snapshots
            snapshots = self.scanner.create_snapshots(markets)
            stored_count = self.store.insert_snapshots(snapshots)
            
            logger.info(f"Stored {stored_count} snapshots")
            
            return IngestionResult(
                success=True,
                markets_scanned=len(markets),
                snapshots_stored=stored_count,
                errors=errors,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            errors.append(str(e))
            
            return IngestionResult(
                success=False,
                markets_scanned=0,
                snapshots_stored=0,
                errors=errors,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
    
    def ingest_series(
        self,
        series_ticker: str,
        min_volume: int = 0,
        status: str = "open"
    ) -> IngestionResult:
        """
        Ingest all markets in a series.
        
        Args:
            series_ticker: Series to ingest (e.g., "KXHIGHNY")
            min_volume: Minimum 24h volume filter
            status: Market status filter
            
        Returns:
            IngestionResult with summary statistics
        """
        start_time = time.time()
        errors = []
        
        logger.info(f"Starting ingestion for series: {series_ticker}")
        
        try:
            # Scan series
            markets = self.scanner.scan_series(
                series_ticker,
                min_volume=min_volume,
                status=status
            )
            logger.info(f"Scanned {len(markets)} markets")
            
            # Create and store snapshots
            snapshots = self.scanner.create_snapshots(markets)
            stored_count = self.store.insert_snapshots(snapshots)
            
            logger.info(f"Stored {stored_count} snapshots")
            
            return IngestionResult(
                success=True,
                markets_scanned=len(markets),
                snapshots_stored=stored_count,
                errors=errors,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            errors.append(str(e))
            
            return IngestionResult(
                success=False,
                markets_scanned=0,
                snapshots_stored=0,
                errors=errors,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
    
    def ingest_top_volume(
        self,
        n: int = 50,
        min_volume: int = 1000
    ) -> IngestionResult:
        """
        Ingest top N markets by volume.
        
        Args:
            n: Number of top markets to ingest
            min_volume: Minimum volume threshold
            
        Returns:
            IngestionResult with summary statistics
        """
        start_time = time.time()
        errors = []
        
        logger.info(f"Starting top volume ingestion (n={n})")
        
        try:
            # Scan top volume markets
            markets = self.scanner.scan_top_volume(n=n, min_volume=min_volume)
            logger.info(f"Scanned {len(markets)} markets")
            
            # Create and store snapshots
            snapshots = self.scanner.create_snapshots(markets)
            stored_count = self.store.insert_snapshots(snapshots)
            
            logger.info(f"Stored {stored_count} snapshots")
            
            return IngestionResult(
                success=True,
                markets_scanned=len(markets),
                snapshots_stored=stored_count,
                errors=errors,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            errors.append(str(e))
            
            return IngestionResult(
                success=False,
                markets_scanned=0,
                snapshots_stored=0,
                errors=errors,
                duration_seconds=time.time() - start_time,
                timestamp=datetime.now()
            )
    
    def ingest_multiple_events(
        self,
        event_tickers: List[str],
        min_volume: int = 0,
        two_sided_only: bool = False
    ) -> List[IngestionResult]:
        """
        Ingest multiple events in sequence.
        
        Args:
            event_tickers: List of events to ingest
            min_volume: Minimum volume filter
            two_sided_only: Only ingest two-sided markets
            
        Returns:
            List of IngestionResult for each event
        """
        results = []
        
        for event_ticker in event_tickers:
            result = self.ingest_event(
                event_ticker,
                min_volume=min_volume,
                two_sided_only=two_sided_only
            )
            results.append(result)
            
            # Small delay between events to avoid rate limiting
            time.sleep(0.5)
        
        return results
    
    # =========================================================================
    # CONTINUOUS INGESTION
    # =========================================================================
    
    def run_continuous(
        self,
        event_tickers: List[str],
        interval_seconds: int = 60,
        max_iterations: Optional[int] = None,
        on_complete: Optional[Callable[[IngestionResult], None]] = None
    ) -> None:
        """
        Run continuous ingestion at specified intervals.
        
        Args:
            event_tickers: Events to monitor
            interval_seconds: Seconds between ingestion runs
            max_iterations: Stop after N iterations (None = run forever)
            on_complete: Callback function after each ingestion
            
        Note:
            This method blocks. Run in a thread for async operation.
        """
        logger.info(
            f"Starting continuous ingestion: {len(event_tickers)} events, "
            f"{interval_seconds}s interval"
        )
        
        iteration = 0
        
        try:
            while max_iterations is None or iteration < max_iterations:
                iteration += 1
                logger.info(f"=== Iteration {iteration} ===")
                
                for event_ticker in event_tickers:
                    result = self.ingest_event(event_ticker, two_sided_only=True)
                    
                    if on_complete:
                        on_complete(result)
                    
                    logger.info(f"  {event_ticker}: {result}")
                
                # Sleep until next iteration
                if max_iterations is None or iteration < max_iterations:
                    logger.debug(f"Sleeping {interval_seconds}s...")
                    time.sleep(interval_seconds)
                    
        except KeyboardInterrupt:
            logger.info("Continuous ingestion stopped by user")
    
    # =========================================================================
    # STATUS AND REPORTING
    # =========================================================================
    
    def get_status(self) -> dict:
        """
        Get current pipeline status.
        
        Returns:
            Dictionary with connection and database status
        """
        status = {
            "db_path": str(self.db_path),
            "api_connected": self._adapter is not None,
            "db_connected": self._store is not None,
        }
        
        if self._store:
            status["db_stats"] = self.store.get_stats()
        
        return status
    
    def close(self) -> None:
        """Close all connections."""
        if self._store:
            self._store.close()
            self._store = None
        
        self._adapter = None
        self._scanner = None
        
        logger.info("Pipeline connections closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def quick_ingest_event(event_ticker: str) -> IngestionResult:
    """
    Quick one-liner to ingest an event.
    
    Example:
        >>> result = quick_ingest_event("KXFEDCHAIRNOM-29")
    """
    with IngestionPipeline() as pipeline:
        return pipeline.ingest_event(event_ticker, two_sided_only=True)


def quick_ingest_series(series_ticker: str) -> IngestionResult:
    """
    Quick one-liner to ingest a series.
    
    Example:
        >>> result = quick_ingest_series("KXHIGHNY")
    """
    with IngestionPipeline() as pipeline:
        return pipeline.ingest_series(series_ticker)


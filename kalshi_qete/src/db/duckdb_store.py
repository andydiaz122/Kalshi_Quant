"""
DuckDB Storage Layer

High-performance analytical database for storing and querying market data.

Why DuckDB?
- Columnar storage optimized for analytical queries
- Zero-copy integration with Polars DataFrames
- ACID transactions for data integrity
- No server setup required (embedded database)
- SQL interface for complex queries

Schema Design:
- orderbook_snapshots: Time-series orderbook data (primary table)
- market_metadata: Slower-changing market info (for joins)
"""

import duckdb
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import polars as pl

from kalshi_qete.src.db.models import (
    OrderbookSnapshot,
    ORDERBOOK_SNAPSHOT_SCHEMA,
    snapshots_to_polars,
)


class DuckDBStore:
    """
    DuckDB storage manager for market data.
    
    Provides methods for:
    - Schema initialization
    - Batch inserts (optimized for time-series data)
    - Queries with Polars DataFrame output
    - Data export/backup
    
    Example:
        >>> store = DuckDBStore("data/qete.duckdb")
        >>> store.insert_snapshots(snapshots)
        >>> df = store.query_snapshots(ticker="KXHIGHNY-25DEC24-T47")
    """
    
    def __init__(self, db_path: Union[str, Path]):
        """
        Initialize DuckDB connection.
        
        Args:
            db_path: Path to database file. Created if doesn't exist.
        """
        self.db_path = Path(db_path)
        
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connect to database
        self.conn = duckdb.connect(str(self.db_path))
        
        # Initialize schema
        self._init_schema()
    
    def _init_schema(self) -> None:
        """Create tables if they don't exist."""
        
        # Main orderbook snapshots table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                snapshot_ts TIMESTAMP NOT NULL,
                ticker VARCHAR NOT NULL,
                series_ticker VARCHAR,
                market_title VARCHAR,
                best_yes_bid DOUBLE,
                best_yes_ask DOUBLE,
                best_no_bid DOUBLE,
                best_no_ask DOUBLE,
                yes_spread DOUBLE,
                no_spread DOUBLE,
                volume_24h BIGINT,
                yes_bid_depth BIGINT,
                no_bid_depth BIGINT,
                
                -- Composite primary key for upsert support
                PRIMARY KEY (snapshot_ts, ticker)
            );
        """)
        
        # Create index for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_ticker 
            ON orderbook_snapshots(ticker);
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_series 
            ON orderbook_snapshots(series_ticker);
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_ts 
            ON orderbook_snapshots(snapshot_ts);
        """)
        
        # Market metadata table (for reference data)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS market_metadata (
                ticker VARCHAR PRIMARY KEY,
                series_ticker VARCHAR,
                event_ticker VARCHAR,
                title VARCHAR,
                status VARCHAR,
                last_updated TIMESTAMP
            );
        """)
    
    # =========================================================================
    # INSERT OPERATIONS
    # =========================================================================
    
    def insert_snapshot(self, snapshot: OrderbookSnapshot) -> None:
        """
        Insert a single orderbook snapshot.
        
        Uses INSERT OR REPLACE for idempotent writes.
        
        Args:
            snapshot: OrderbookSnapshot to insert
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO orderbook_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            snapshot.snapshot_ts,
            snapshot.ticker,
            snapshot.series_ticker,
            snapshot.market_title,
            snapshot.best_yes_bid,
            snapshot.best_yes_ask,
            snapshot.best_no_bid,
            snapshot.best_no_ask,
            snapshot.yes_spread,
            snapshot.no_spread,
            snapshot.volume_24h,
            snapshot.yes_bid_depth,
            snapshot.no_bid_depth,
        ])
    
    def insert_snapshots(self, snapshots: List[OrderbookSnapshot]) -> int:
        """
        Batch insert multiple snapshots efficiently.
        
        Uses Polars DataFrame for zero-copy transfer to DuckDB.
        
        Args:
            snapshots: List of OrderbookSnapshot objects
            
        Returns:
            Number of rows inserted
        """
        if not snapshots:
            return 0
        
        # Convert to Polars DataFrame
        df = snapshots_to_polars(snapshots)
        
        # Insert via DuckDB's native Polars integration
        self.conn.execute("""
            INSERT OR REPLACE INTO orderbook_snapshots 
            SELECT * FROM df
        """)
        
        return len(snapshots)
    
    def insert_from_polars(self, df: pl.DataFrame) -> int:
        """
        Insert directly from a Polars DataFrame.
        
        DataFrame must match ORDERBOOK_SNAPSHOT_SCHEMA.
        
        Args:
            df: Polars DataFrame with orderbook data
            
        Returns:
            Number of rows inserted
        """
        if df.is_empty():
            return 0
        
        self.conn.execute("""
            INSERT OR REPLACE INTO orderbook_snapshots 
            SELECT * FROM df
        """)
        
        return len(df)
    
    # =========================================================================
    # QUERY OPERATIONS
    # =========================================================================
    
    def query_snapshots(
        self,
        ticker: Optional[str] = None,
        series_ticker: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pl.DataFrame:
        """
        Query orderbook snapshots with filters.
        
        Returns a Polars DataFrame for efficient analysis.
        
        Args:
            ticker: Filter by exact ticker
            series_ticker: Filter by series
            start_time: Filter snapshots after this time
            end_time: Filter snapshots before this time
            limit: Maximum rows to return
            
        Returns:
            Polars DataFrame with matching snapshots
        """
        conditions = []
        params = []
        
        if ticker:
            conditions.append("ticker = ?")
            params.append(ticker)
        
        if series_ticker:
            conditions.append("series_ticker = ?")
            params.append(series_ticker)
        
        if start_time:
            conditions.append("snapshot_ts >= ?")
            params.append(start_time)
        
        if end_time:
            conditions.append("snapshot_ts <= ?")
            params.append(end_time)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        query = f"""
            SELECT * FROM orderbook_snapshots
            WHERE {where_clause}
            ORDER BY snapshot_ts DESC
            {limit_clause}
        """
        
        return self.conn.execute(query, params).pl()
    
    def get_latest_snapshot(self, ticker: str) -> Optional[pl.DataFrame]:
        """
        Get the most recent snapshot for a ticker.
        
        Args:
            ticker: Market ticker
            
        Returns:
            Single-row Polars DataFrame or None
        """
        result = self.conn.execute("""
            SELECT * FROM orderbook_snapshots
            WHERE ticker = ?
            ORDER BY snapshot_ts DESC
            LIMIT 1
        """, [ticker]).pl()
        
        return result if not result.is_empty() else None
    
    def get_ticker_history(
        self,
        ticker: str,
        hours: int = 24
    ) -> pl.DataFrame:
        """
        Get snapshot history for a ticker over the last N hours.
        
        Args:
            ticker: Market ticker
            hours: Hours of history to retrieve
            
        Returns:
            Polars DataFrame with time-series data
        """
        return self.conn.execute("""
            SELECT * FROM orderbook_snapshots
            WHERE ticker = ?
              AND snapshot_ts >= NOW() - INTERVAL ? HOUR
            ORDER BY snapshot_ts ASC
        """, [ticker, hours]).pl()
    
    def get_series_summary(self, series_ticker: str) -> pl.DataFrame:
        """
        Get summary statistics for all markets in a series.
        
        Args:
            series_ticker: Series to summarize
            
        Returns:
            DataFrame with one row per ticker showing latest values
        """
        return self.conn.execute("""
            WITH latest AS (
                SELECT 
                    ticker,
                    MAX(snapshot_ts) as latest_ts
                FROM orderbook_snapshots
                WHERE series_ticker = ?
                GROUP BY ticker
            )
            SELECT s.*
            FROM orderbook_snapshots s
            JOIN latest l ON s.ticker = l.ticker AND s.snapshot_ts = l.latest_ts
            ORDER BY s.best_yes_bid DESC
        """, [series_ticker]).pl()
    
    # =========================================================================
    # ANALYTICS QUERIES
    # =========================================================================
    
    def get_spread_history(
        self,
        ticker: str,
        hours: int = 24
    ) -> pl.DataFrame:
        """
        Get spread history for analysis.
        
        Args:
            ticker: Market ticker
            hours: Hours of history
            
        Returns:
            DataFrame with timestamp, yes_spread, no_spread
        """
        return self.conn.execute("""
            SELECT 
                snapshot_ts,
                yes_spread,
                no_spread,
                (yes_spread + no_spread) / 2 as avg_spread
            FROM orderbook_snapshots
            WHERE ticker = ?
              AND snapshot_ts >= NOW() - INTERVAL ? HOUR
            ORDER BY snapshot_ts ASC
        """, [ticker, hours]).pl()
    
    def get_volume_by_series(self) -> pl.DataFrame:
        """
        Get total volume aggregated by series.
        
        Returns:
            DataFrame with series_ticker, total_volume, market_count
        """
        return self.conn.execute("""
            SELECT 
                series_ticker,
                COUNT(DISTINCT ticker) as market_count,
                SUM(volume_24h) as total_volume_24h,
                MAX(snapshot_ts) as latest_snapshot
            FROM orderbook_snapshots
            WHERE series_ticker IS NOT NULL
            GROUP BY series_ticker
            ORDER BY total_volume_24h DESC
        """).pl()
    
    # =========================================================================
    # MAINTENANCE OPERATIONS
    # =========================================================================
    
    def get_stats(self) -> dict:
        """
        Get database statistics.
        
        Returns:
            Dictionary with table counts and date ranges
        """
        snapshot_count = self.conn.execute(
            "SELECT COUNT(*) FROM orderbook_snapshots"
        ).fetchone()[0]
        
        date_range = self.conn.execute("""
            SELECT MIN(snapshot_ts), MAX(snapshot_ts) 
            FROM orderbook_snapshots
        """).fetchone()
        
        unique_tickers = self.conn.execute(
            "SELECT COUNT(DISTINCT ticker) FROM orderbook_snapshots"
        ).fetchone()[0]
        
        unique_series = self.conn.execute(
            "SELECT COUNT(DISTINCT series_ticker) FROM orderbook_snapshots"
        ).fetchone()[0]
        
        return {
            "snapshot_count": snapshot_count,
            "unique_tickers": unique_tickers,
            "unique_series": unique_series,
            "earliest_snapshot": date_range[0],
            "latest_snapshot": date_range[1],
            "db_path": str(self.db_path),
        }
    
    def vacuum(self) -> None:
        """
        Optimize database storage.
        
        Reclaims space from deleted rows and optimizes indexes.
        """
        self.conn.execute("VACUUM")
    
    def export_to_parquet(self, output_path: Union[str, Path]) -> None:
        """
        Export all snapshots to Parquet file.
        
        Parquet is efficient for backup and sharing.
        
        Args:
            output_path: Path for output .parquet file
        """
        self.conn.execute(f"""
            COPY orderbook_snapshots TO '{output_path}' (FORMAT PARQUET)
        """)
    
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


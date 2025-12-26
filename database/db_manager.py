"""
Database manager for DuckDB operations.
Handles schema creation and streaming inserts for orderbook snapshots.
"""
import duckdb
from typing import List, Optional
from datetime import datetime
from models.market_data import OrderbookSnapshot


class DatabaseManager:
    """Manages DuckDB connection and operations for market data storage."""
    
    def __init__(self, db_path: str):
        """
        Initialize DuckDB connection.
        
        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.initialize_schema()
    
    def initialize_schema(self) -> None:
        """Create orderbook_snapshots table if it doesn't exist."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            snapshot_timestamp TIMESTAMP NOT NULL,
            ticker VARCHAR NOT NULL,
            market_title VARCHAR,
            series_ticker VARCHAR,
            best_yes_bid REAL,
            best_yes_ask REAL,
            best_no_bid REAL,
            best_no_ask REAL,
            yes_spread REAL,
            no_spread REAL,
            volume_24h INTEGER,
            PRIMARY KEY (snapshot_timestamp, ticker)
        );
        """
        self.conn.execute(schema_sql)
    
    def insert_snapshot(self, snapshot: OrderbookSnapshot) -> None:
        """
        Stream insert single orderbook snapshot (no DataFrame).
        
        Args:
            snapshot: OrderbookSnapshot object to insert
        """
        insert_sql = """
        INSERT INTO orderbook_snapshots (
            snapshot_timestamp, ticker, market_title, series_ticker,
            best_yes_bid, best_yes_ask, best_no_bid, best_no_ask,
            yes_spread, no_spread, volume_24h
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.conn.execute(
            insert_sql,
            [
                snapshot.snapshot_timestamp,
                snapshot.ticker,
                snapshot.market_title,
                snapshot.series_ticker,
                snapshot.best_yes_bid,
                snapshot.best_yes_ask,
                snapshot.best_no_bid,
                snapshot.best_no_ask,
                snapshot.yes_spread,
                snapshot.no_spread,
                snapshot.volume_24h,
            ]
        )
    
    def insert_snapshots_batch(self, snapshots: List[OrderbookSnapshot]) -> None:
        """
        Batch insert multiple orderbook snapshots using executemany for efficiency.
        
        Args:
            snapshots: List of OrderbookSnapshot objects to insert
        """
        if not snapshots:
            return
        
        insert_sql = """
        INSERT INTO orderbook_snapshots (
            snapshot_timestamp, ticker, market_title, series_ticker,
            best_yes_bid, best_yes_ask, best_no_bid, best_no_ask,
            yes_spread, no_spread, volume_24h
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Prepare data as list of tuples for executemany
        data = [
            (
                snapshot.snapshot_timestamp,
                snapshot.ticker,
                snapshot.market_title,
                snapshot.series_ticker,
                snapshot.best_yes_bid,
                snapshot.best_yes_ask,
                snapshot.best_no_bid,
                snapshot.best_no_ask,
                snapshot.yes_spread,
                snapshot.no_spread,
                snapshot.volume_24h,
            )
            for snapshot in snapshots
        ]
        
        self.conn.executemany(insert_sql, data)
    
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
    
    def insert_snapshot_safe(self, snapshot: OrderbookSnapshot) -> None:
        """
        Insert a single snapshot with automatic connection management.
        Opens and closes connection for each write to minimize memory usage.
        This ensures data is persisted even if the script crashes.
        
        Args:
            snapshot: OrderbookSnapshot object to insert
        """
        # Create a new connection for this write operation
        # This ensures the connection is closed immediately after the write
        conn = duckdb.connect(self.db_path)
        try:
            # Ensure schema exists (in case this is the first write)
            schema_sql = """
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                snapshot_timestamp TIMESTAMP NOT NULL,
                ticker VARCHAR NOT NULL,
                market_title VARCHAR,
                series_ticker VARCHAR,
                best_yes_bid REAL,
                best_yes_ask REAL,
                best_no_bid REAL,
                best_no_ask REAL,
                yes_spread REAL,
                no_spread REAL,
                volume_24h INTEGER,
                PRIMARY KEY (snapshot_timestamp, ticker)
            );
            """
            conn.execute(schema_sql)
            
            # Insert the snapshot
            insert_sql = """
            INSERT INTO orderbook_snapshots (
                snapshot_timestamp, ticker, market_title, series_ticker,
                best_yes_bid, best_yes_ask, best_no_bid, best_no_ask,
                yes_spread, no_spread, volume_24h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            conn.execute(
                insert_sql,
                [
                    snapshot.snapshot_timestamp,
                    snapshot.ticker,
                    snapshot.market_title,
                    snapshot.series_ticker,
                    snapshot.best_yes_bid,
                    snapshot.best_yes_ask,
                    snapshot.best_no_bid,
                    snapshot.best_no_ask,
                    snapshot.yes_spread,
                    snapshot.no_spread,
                    snapshot.volume_24h,
                ]
            )
        finally:
            # Always close the connection, even if an error occurs
            conn.close()


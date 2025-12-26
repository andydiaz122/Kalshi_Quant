"""
Main entry point for Kalshi Quant data ingestion system.
"""
from kalshi_python_sync import KalshiClient, Configuration
from kalshi_python_sync.auth import KalshiAuth
from kalshi_python_sync.exceptions import UnauthorizedException, ApiException

from config.settings import KEY_ID, KEY_FILE_PATH, DATABASE_PATH, MIN_DAILY_VOLUME
from utils.auth import load_private_key_pem
from database.db_manager import DatabaseManager
from ingestion.market_scanner import MarketScanner


def main():
    """
    Main execution function: initialize components and scan markets.
    """
    # Validate key ID is set
    if KEY_ID == "YOUR_KEY_ID_HERE":
        print("❌ ERROR: Please set KEY_ID constant with your actual Kalshi API Key ID")
        return
    
    try:
        # Load private key
        print("--- 🔌 INITIALIZING KALSHI CONNECTION ---")
        private_key_pem = load_private_key_pem(KEY_FILE_PATH)
        print(f"✅ Loaded private key from '{KEY_FILE_PATH}'")
        
        # Initialize configuration
        config = Configuration()
        
        # Initialize KalshiClient with authentication
        client = KalshiClient(configuration=config)
        
        # Set up Kalshi authentication
        client.kalshi_auth = KalshiAuth(KEY_ID, private_key_pem)
        print("✅ Authentication configured")
        
        # Initialize DatabaseManager
        print(f"\n--- 💾 INITIALIZING DATABASE: {DATABASE_PATH} ---")
        db_manager = DatabaseManager(DATABASE_PATH)
        print("✅ Database initialized")
        
        # Initialize MarketScanner
        scanner = MarketScanner(client)
        
        # Scan and store markets
        print(f"\n--- 📊 SCANNING MARKETS (MIN VOLUME: ${MIN_DAILY_VOLUME/100:.2f}) ---")
        scanner.scan_and_store_markets(
            series_ticker="KXFEDDECISION",
            db_manager=db_manager,
            min_volume=MIN_DAILY_VOLUME
        )
        
        # Close database connection
        db_manager.close()
        
        print("\n✅ SUCCESS: Data ingestion complete")
        
    except FileNotFoundError as e:
        print(f"\n{e}")
    except UnauthorizedException as e:
        print(f"\n❌ AUTHENTICATION ERROR: {e}")
        print("   -> Please verify your KEY_ID and private key file are correct")
    except ApiException as e:
        print(f"\n❌ API ERROR: {e}")
        print("   -> Check your network connection and API status")
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        print(f"   -> Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


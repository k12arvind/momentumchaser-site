#!/usr/bin/env python3
"""
Fetch last 10 trading days of OHLC data from Kite Connect and store in database.
This script helps bootstrap the database with recent data for analysis.
"""

import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
import logging

from kite_client import get_kite, instruments_nse_eq, get_hist_daily
from database import store_ohlc_data, get_available_dates

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_universe(path: str = "data/universe_nifty500.txt") -> List[str]:
    """Load stock symbols from universe file."""
    if not os.path.exists(path):
        logger.error(f"Universe file not found: {path}")
        logger.info("Run: python scripts/update_universe.py")
        sys.exit(1)
    
    with open(path) as f:
        symbols = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    
    logger.info(f"Loaded {len(symbols)} symbols from {path}")
    return symbols

def get_trading_days(end_date: date, num_days: int = 10) -> List[date]:
    """
    Get the last N trading days (excluding weekends).
    Note: This doesn't account for market holidays - only weekends.
    """
    trading_days = []
    current_date = end_date
    
    while len(trading_days) < num_days:
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() < 5:  # Monday=0 to Friday=4
            trading_days.append(current_date)
        current_date -= timedelta(days=1)
    
    trading_days.reverse()  # Return in chronological order
    return trading_days

def fetch_and_store_recent_data(
    symbols: List[str], 
    trading_days: List[date],
    pause_seconds: float = 0.15,
    batch_size: int = 50
) -> Dict[str, Any]:
    """
    Fetch OHLC data for symbols over the specified trading days and store in database.
    """
    logger.info(f"Fetching data for {len(symbols)} symbols over {len(trading_days)} trading days")
    logger.info(f"Date range: {trading_days[0]} to {trading_days[-1]}")
    
    kite = get_kite()
    inst_df = instruments_nse_eq(kite)
    by_symbol = {r["tradingsymbol"]: r["instrument_token"] for _, r in inst_df.iterrows()}
    
    start_date = trading_days[0]
    end_date = trading_days[-1]
    
    stats = {
        'total_symbols': len(symbols),
        'successful_fetches': 0,
        'failed_fetches': 0,
        'total_records_stored': 0,
        'symbols_with_data': 0,
        'fetch_errors': []
    }
    
    for i, symbol in enumerate(symbols, 1):
        logger.info(f"[{i}/{len(symbols)}] Processing {symbol}")
        
        # Get instrument token
        token = by_symbol.get(symbol)
        if not token:
            logger.warning(f"Instrument token not found for {symbol}")
            stats['failed_fetches'] += 1
            stats['fetch_errors'].append(f"{symbol}: instrument_token_not_found")
            continue
        
        try:
            # Fetch historical data directly (not using cache for this bootstrap)
            df = get_hist_daily(kite, token, start_date, end_date)
            
            if df.empty:
                logger.warning(f"No data returned for {symbol}")
                stats['failed_fetches'] += 1
                stats['fetch_errors'].append(f"{symbol}: no_data_returned")
                continue
            
            # Filter to only the trading days we want
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[df['date'].isin(trading_days)]
            
            if df.empty:
                logger.warning(f"No data for trading days for {symbol}")
                stats['failed_fetches'] += 1
                stats['fetch_errors'].append(f"{symbol}: no_trading_day_data")
                continue
            
            # Store in database
            records_stored = store_ohlc_data(symbol, df)
            
            if records_stored > 0:
                stats['successful_fetches'] += 1
                stats['total_records_stored'] += records_stored
                stats['symbols_with_data'] += 1
                logger.info(f"Stored {records_stored} records for {symbol}")
            else:
                logger.warning(f"No records stored for {symbol}")
                stats['failed_fetches'] += 1
                stats['fetch_errors'].append(f"{symbol}: database_storage_failed")
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            stats['failed_fetches'] += 1
            stats['fetch_errors'].append(f"{symbol}: {str(e)}")
        
        # Rate limiting
        time.sleep(pause_seconds)
        
        # Progress update every batch
        if i % batch_size == 0:
            success_rate = (stats['successful_fetches'] / i) * 100
            logger.info(f"Progress: {i}/{len(symbols)} symbols processed, {success_rate:.1f}% success rate")
    
    return stats

def verify_data_storage(symbols: List[str], trading_days: List[date]) -> Dict[str, Any]:
    """Verify that data was stored correctly in the database."""
    logger.info("Verifying data storage...")
    
    available_dates = get_available_dates()
    stored_trading_days = [d for d in [td.isoformat() for td in trading_days] if d in available_dates]
    
    verification = {
        'requested_dates': [td.isoformat() for td in trading_days],
        'available_dates': available_dates[:10],  # Show first 10
        'stored_trading_days': stored_trading_days,
        'missing_dates': [td.isoformat() for td in trading_days if td.isoformat() not in available_dates],
        'coverage_percentage': (len(stored_trading_days) / len(trading_days)) * 100
    }
    
    logger.info(f"Data coverage: {verification['coverage_percentage']:.1f}%")
    if verification['missing_dates']:
        logger.warning(f"Missing dates: {verification['missing_dates']}")
    
    return verification

def main():
    """Main function to fetch and store recent trading data."""
    logger.info("Starting recent data fetch...")
    
    # Configuration
    num_trading_days = int(os.environ.get("FETCH_DAYS", "10"))
    pause_seconds = float(os.environ.get("PAUSE_SECONDS", "0.15"))
    universe_path = os.environ.get("UNIVERSE_PATH", "data/universe_nifty500.txt")
    
    # Load symbols
    symbols = load_universe(universe_path)
    
    # Calculate trading days
    end_date = date.today()
    trading_days = get_trading_days(end_date, num_trading_days)
    
    logger.info(f"Target trading days: {[td.isoformat() for td in trading_days]}")
    
    # Check what data already exists
    existing_dates = get_available_dates()
    if existing_dates:
        logger.info(f"Database already contains data for {len(existing_dates)} dates")
        logger.info(f"Latest date: {existing_dates[0] if existing_dates else 'None'}")
        
        # Ask user if they want to continue
        response = input(f"Fetch data for last {num_trading_days} trading days? (y/n): ").lower().strip()
        if response != 'y':
            logger.info("Fetch cancelled by user")
            return
    
    # Fetch and store data
    start_time = time.time()
    
    try:
        stats = fetch_and_store_recent_data(symbols, trading_days, pause_seconds)
        
        # Print results
        duration = time.time() - start_time
        logger.info("\n" + "="*60)
        logger.info("FETCH COMPLETED")
        logger.info("="*60)
        logger.info(f"Duration: {duration:.1f} seconds")
        logger.info(f"Total symbols processed: {stats['total_symbols']}")
        logger.info(f"Successful fetches: {stats['successful_fetches']}")
        logger.info(f"Failed fetches: {stats['failed_fetches']}")
        logger.info(f"Success rate: {(stats['successful_fetches']/stats['total_symbols'])*100:.1f}%")
        logger.info(f"Total records stored: {stats['total_records_stored']}")
        logger.info(f"Symbols with data: {stats['symbols_with_data']}")
        
        # Show some sample errors
        if stats['fetch_errors']:
            logger.info(f"\nSample errors ({min(5, len(stats['fetch_errors']))}):")
            for error in stats['fetch_errors'][:5]:
                logger.info(f"  - {error}")
        
        # Verify storage
        verification = verify_data_storage(symbols, trading_days)
        logger.info(f"\nDatabase verification:")
        logger.info(f"Requested {len(trading_days)} dates, stored {len(verification['stored_trading_days'])} dates")
        logger.info(f"Data coverage: {verification['coverage_percentage']:.1f}%")
        
        logger.info("\n" + "="*60)
        logger.info("Next steps:")
        logger.info("1. Run: python src/api_server.py")
        logger.info("2. Run: python src/publish_site.py")
        logger.info("3. Open site/index.html to explore the data")
        logger.info("="*60)
        
    except KeyboardInterrupt:
        logger.info("\nFetch interrupted by user")
    except Exception as e:
        logger.error(f"Fetch failed with error: {e}")
        raise

if __name__ == "__main__":
    main()
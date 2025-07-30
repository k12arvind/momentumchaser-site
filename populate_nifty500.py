#!/usr/bin/env python3
"""
Populate database with all NIFTY 500 stocks - optimized version
"""
import sys
import os
sys.path.append('src')

from kite_client import get_kite, instruments_nse_eq
from database import store_ohlc_data, init_database
import pandas as pd
from datetime import datetime, timedelta
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_trading_days(start_date, end_date, num_days=10):
    """Calculate the last N trading days (excluding weekends)"""
    trading_days = []
    current_date = end_date
    
    while len(trading_days) < num_days and current_date >= start_date:
        # Skip weekends (Monday=0, Sunday=6)
        if current_date.weekday() < 5:  # Monday to Friday
            trading_days.append(current_date.strftime('%Y-%m-%d'))
        current_date -= timedelta(days=1)
    
    return list(reversed(trading_days))

def main():
    """Populate database with all NIFTY 500 stocks"""
    
    logger.info("Starting NIFTY 500 data population...")
    
    # Initialize database
    init_database()
    
    # Get kite client
    try:
        kite = get_kite()
        logger.info("Kite client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kite client: {e}")
        return
    
    # Load NIFTY 500 symbols
    try:
        with open('data/universe_nifty500.txt', 'r') as f:
            nifty500_symbols = [line.strip() for line in f.readlines()]
        logger.info(f"Loaded {len(nifty500_symbols)} NIFTY 500 symbols")
    except Exception as e:
        logger.error(f"Failed to load NIFTY 500 symbols: {e}")
        return
    
    # Get instruments for token lookup
    logger.info("Loading NSE instruments...")
    instruments_df = instruments_nse_eq(kite)
    instruments_dict = dict(zip(instruments_df['tradingsymbol'], instruments_df['instrument_token']))
    logger.info(f"Loaded {len(instruments_dict)} NSE instruments")
    
    # Calculate date range (last 15 calendar days to ensure 10 trading days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=20)  # Get 20 days to ensure 10 trading days
    
    target_trading_days = get_trading_days(start_date, end_date, 10)
    logger.info(f"Target trading days: {target_trading_days}")
    logger.info(f"Fetching data for {len(nifty500_symbols)} symbols over {len(target_trading_days)} trading days")
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Process symbols
    success_count = 0
    error_count = 0
    already_exists_count = 0
    
    for i, symbol in enumerate(nifty500_symbols, 1):
        try:
            logger.info(f"[{i}/{len(nifty500_symbols)}] Processing {symbol}")
            
            # Check if symbol exists in instruments
            if symbol not in instruments_dict:
                logger.warning(f"Symbol {symbol} not found in instruments")
                error_count += 1
                continue
            
            token = instruments_dict[symbol]
            
            # Fetch historical data
            data = kite.historical_data(
                instrument_token=token,
                from_date=start_date.strftime('%Y-%m-%d'),
                to_date=end_date.strftime('%Y-%m-%d'),
                interval="day"
            )
            
            if not data:
                logger.warning(f"No data received for {symbol}")
                error_count += 1
                continue
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            logger.info(f"Received {len(df)} records for {symbol}")
            
            # Store in database
            records_stored = store_ohlc_data(symbol, df)
            if records_stored > 0:
                success_count += 1
                logger.info(f"✅ Stored {records_stored} records for {symbol}")
            else:
                already_exists_count += 1
                logger.info(f"⚠️  Data already exists for {symbol}")
            
            # Add small delay to avoid rate limiting
            time.sleep(0.1)
            
            # Progress report every 50 symbols
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(nifty500_symbols)} symbols processed")
                logger.info(f"Success: {success_count}, Already exists: {already_exists_count}, Errors: {error_count}")
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            error_count += 1
            continue
    
    # Final summary
    logger.info("=" * 60)
    logger.info("NIFTY 500 DATA POPULATION COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Total symbols processed: {len(nifty500_symbols)}")
    logger.info(f"Successfully added: {success_count}")
    logger.info(f"Already existed: {already_exists_count}")
    logger.info(f"Errors: {error_count}")
    
    # Show final database status
    import sqlite3
    conn = sqlite3.connect('data/stock_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT symbol) as total_symbols FROM daily_ohlc")
    total_symbols = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) as total_records FROM daily_ohlc")
    total_records = cursor.fetchone()[0]
    conn.close()
    
    logger.info(f"Database now contains {total_symbols} unique symbols with {total_records} total records")
    logger.info("🚀 Your 10-day OHLC feature is now ready for all NIFTY 500 stocks!")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Add more symbols to the database for testing
"""
import sys
import os
sys.path.append('src')

from kite_client import get_kite, instruments_nse_eq
from database import store_ohlc_data, init_database
import pandas as pd
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Add popular symbols from the scan results"""
    
    # Initialize database
    init_database()
    
    # Get kite client
    try:
        kite = get_kite()
        logger.info("Kite client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kite client: {e}")
        return
    
    # Get instruments for token lookup
    instruments_df = instruments_nse_eq(kite)
    instruments_dict = dict(zip(instruments_df['tradingsymbol'], instruments_df['instrument_token']))
    
    # Popular symbols that are likely on the website
    popular_symbols = [
        'HDFCBANK', 'ICICIBANK', 'KOTAKBANK', 'AXISBANK',
        'WIPRO', 'HDFC', 'ITC', 'HINDUNILVR', 'MARUTI',
        'BAJFINANCE', 'ASIANPAINT', 'LTIM', 'TITAN',
        'NESTLEIND', 'BHARTIARTL', 'SBIN', 'LT'
    ]
    
    # Calculate date range (last 10 trading days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=15)  # Get 15 days to ensure 10 trading days
    
    logger.info(f"Adding symbols: {popular_symbols}")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    
    success_count = 0
    for symbol in popular_symbols:
        try:
            if symbol not in instruments_dict:
                logger.warning(f"Symbol {symbol} not found in instruments")
                continue
                
            token = instruments_dict[symbol]
            logger.info(f"Fetching data for {symbol} (token: {token})")
            
            # Fetch historical data
            data = kite.historical_data(
                token,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                "day"
            )
            
            if not data:
                logger.warning(f"No data received for {symbol}")
                continue
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            logger.info(f"Received {len(df)} records for {symbol}")
            
            # Store in database
            records_stored = store_ohlc_data(symbol, df)
            if records_stored > 0:
                success_count += 1
                logger.info(f"✅ Stored records for {symbol}")
            else:
                logger.info(f"⚠️  Data already exists for {symbol}")
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
    
    logger.info(f"Successfully added {success_count} new symbols")
    
    # Show current database status
    import sqlite3
    conn = sqlite3.connect('data/stock_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, COUNT(*) as count FROM daily_ohlc GROUP BY symbol ORDER BY symbol")
    results = cursor.fetchall()
    conn.close()
    
    logger.info("Current database contents:")
    for symbol, count in results:
        logger.info(f"  {symbol}: {count} records")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Test script to fetch and store a few symbols to verify database functionality
"""
import sys
import os
sys.path.append('src')

from kite_client import get_kite
from database import store_ohlc_data, init_database
import pandas as pd
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Test with just a few symbols"""
    
    # Initialize database
    init_database()
    
    # Get kite client
    try:
        kite = get_kite()
        logger.info("Kite client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kite client: {e}")
        return
    
    # Test symbols
    test_symbols = ['RELIANCE', 'TCS', 'INFY']
    
    # Calculate date range (last 5 trading days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)
    
    logger.info(f"Testing with symbols: {test_symbols}")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    
    for symbol in test_symbols:
        try:
            logger.info(f"Fetching data for {symbol}")
            
            # Fetch historical data
            data = kite.historical_data(
                kite.ltp(f"NSE:{symbol}")[f"NSE:{symbol}"]["instrument_token"],
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
            logger.info(f"Data columns: {df.columns.tolist()}")
            logger.info(f"Sample data: {df.head(1).to_dict('records')}")
            
            # Store in database
            records_stored = store_ohlc_data(symbol, df)
            logger.info(f"Stored {records_stored} records for {symbol}")
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            import traceback
            traceback.print_exc()
    
    logger.info("Test completed")

if __name__ == "__main__":
    main()
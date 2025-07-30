#!/usr/bin/env python3
"""
Add PTCIL specifically
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
    """Add PTCIL symbol"""
    
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
    
    symbol = 'PTCIL'
    
    # Calculate date range (last 10 trading days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=15)  # Get 15 days to ensure 10 trading days
    
    logger.info(f"Adding symbol: {symbol}")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    
    try:
        if symbol not in instruments_dict:
            logger.error(f"Symbol {symbol} not found in instruments")
            # Let's search for similar symbols
            similar = instruments_df[instruments_df['tradingsymbol'].str.contains('PTC', case=False)]
            logger.info("Similar symbols found:")
            for _, row in similar.iterrows():
                logger.info(f"  {row['tradingsymbol']} - {row['name']}")
            return
            
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
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        logger.info(f"Received {len(df)} records for {symbol}")
        
        # Store in database
        records_stored = store_ohlc_data(symbol, df)
        logger.info(f"Stored records for {symbol}")
        
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
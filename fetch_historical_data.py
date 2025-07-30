#!/usr/bin/env python3
"""
Fetch 200+ trading days of historical data for all symbols to enable accurate EMA calculations.
This will fetch OHLCV data for consolidation analysis and volume contraction features.
"""

import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from src.kite_client import get_kite
from src.database import store_ohlc_data, get_symbols_for_date, get_available_dates
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_trading_days_back(days=250):
    """Calculate start date to get approximately 'days' trading days of data."""
    # Account for weekends and holidays - multiply by ~1.4 to get trading days
    calendar_days = int(days * 1.4)
    start_date = datetime.now() - timedelta(days=calendar_days)
    return start_date.strftime('%Y-%m-%d')

def fetch_symbol_historical_data(kite, symbol, start_date, end_date):
    """Fetch historical data for a single symbol."""
    try:
        # Get instrument token for the symbol
        instruments = kite.instruments('NSE')
        instrument = None
        
        for inst in instruments:
            if inst['tradingsymbol'] == symbol and inst['segment'] == 'NSE':
                instrument = inst
                break
        
        if not instrument:
            logging.error(f"Instrument not found for symbol: {symbol}")
            return None
        
        instrument_token = instrument['instrument_token']
        logging.info(f"Fetching data for {symbol} (token: {instrument_token}) from {start_date} to {end_date}")
        
        # Fetch historical data
        historical_data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=start_date,
            to_date=end_date,
            interval='day'
        )
        
        if not historical_data:
            logging.warning(f"No historical data returned for {symbol}")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(historical_data)
        
        # Rename columns to match our database schema
        df = df.rename(columns={
            'date': 'date',
            'open': 'open',
            'high': 'high', 
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })
        
        # Ensure we have the required columns
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df.columns:
                logging.error(f"Missing required column {col} for {symbol}")
                return None
        
        logging.info(f"Fetched {len(df)} days of data for {symbol}")
        return df[required_columns]
        
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None

def fetch_all_historical_data(trading_days=200):
    """Fetch historical data for all symbols in the database."""
    try:
        # Get Kite client
        kite = get_kite()
        if not kite:
            logging.error("Failed to initialize Kite client")
            return False
        
        # Get list of symbols from most recent date
        dates = get_available_dates()
        if not dates:
            logging.error("No dates available in database")
            return False
        
        latest_date = dates[0]
        symbols = get_symbols_for_date(latest_date)
        logging.info(f"Found {len(symbols)} symbols to process")
        
        # Calculate date range for fetching historical data
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = get_trading_days_back(trading_days)
        
        logging.info(f"Fetching {trading_days} trading days of data from {start_date} to {end_date}")
        
        # Process symbols in batches to avoid rate limiting
        batch_size = 10
        success_count = 0
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logging.info(f"Processing batch {i//batch_size + 1}/{(len(symbols) + batch_size - 1)//batch_size}")
            
            for symbol in batch:
                try:
                    # Fetch historical data
                    df = fetch_symbol_historical_data(kite, symbol, start_date, end_date)
                    
                    if df is not None and not df.empty:
                        # Store in database
                        rows_stored = store_ohlc_data(symbol, df)
                        if rows_stored > 0:
                            success_count += 1
                            logging.info(f"✅ {symbol}: Stored {len(df)} days of data")
                        else:
                            logging.warning(f"⚠️  {symbol}: Data fetched but not stored")
                    else:
                        logging.warning(f"❌ {symbol}: No data fetched")
                    
                    # Rate limiting - pause between requests
                    time.sleep(0.5)  # 500ms delay
                    
                except Exception as e:
                    logging.error(f"Error processing {symbol}: {e}")
                    continue
            
            # Longer pause between batches
            if i + batch_size < len(symbols):
                logging.info("Pausing 3 seconds between batches...")
                time.sleep(3)
        
        logging.info(f"Historical data fetch complete. Success: {success_count}/{len(symbols)} symbols")
        
        # Verify data sufficiency
        verify_data_sufficiency(symbols[:5])  # Check first 5 symbols
        
        return success_count > 0
        
    except Exception as e:
        logging.error(f"Error in fetch_all_historical_data: {e}")
        return False

def verify_data_sufficiency(sample_symbols):
    """Verify that we have sufficient data for accurate EMA calculations."""
    logging.info("Verifying data sufficiency for EMA calculations...")
    
    for symbol in sample_symbols:
        try:
            from src.database import get_ohlc_data
            df = get_ohlc_data(symbol)
            
            days_available = len(df)
            date_range = (df['date'].max() - df['date'].min()).days if len(df) > 1 else 0
            
            logging.info(f"{symbol}: {days_available} data points, {date_range} calendar days")
            
            # Check EMA calculation feasibility
            ema_status = {
                'EMA 4': '✅' if days_available >= 10 else '❌',
                'EMA 9': '✅' if days_available >= 20 else '❌', 
                'EMA 18': '✅' if days_available >= 35 else '❌',
                'EMA 50': '✅' if days_available >= 75 else '❌',
                'EMA 200': '✅' if days_available >= 250 else '❌'
            }
            
            for ema, status in ema_status.items():
                logging.info(f"  {ema}: {status}")
                
        except Exception as e:
            logging.error(f"Error verifying {symbol}: {e}")

if __name__ == '__main__':
    print("🚀 Starting historical data fetch for accurate EMA calculations...")
    print("This will fetch 200+ trading days of OHLCV data for all symbols.")
    print("Estimated time: 15-30 minutes depending on number of symbols and API limits.")
    print()
    
    # Ask for confirmation
    if len(sys.argv) > 1 and sys.argv[1] == '--auto':
        proceed = True
    else:
        proceed = input("Proceed with historical data fetch? (y/N): ").lower() == 'y'
    
    if proceed:
        success = fetch_all_historical_data(trading_days=200)
        if success:
            print("\n✅ Historical data fetch completed successfully!")
            print("Next steps:")
            print("1. Run: python calculate_emas.py  # Recalculate EMAs with new data")
            print("2. Run: python export_ema_data.py  # Export updated EMA data") 
            print("3. Run: python src/publish_site.py  # Regenerate website")
        else:
            print("\n❌ Historical data fetch failed. Check logs for errors.")
    else:
        print("Historical data fetch cancelled.")
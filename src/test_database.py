#!/usr/bin/env python3
"""
Test script to validate database functionality before running the main data fetch.
"""

import pandas as pd
from datetime import date, timedelta
from database import (
    init_database, 
    store_ohlc_data, 
    get_ohlc_data, 
    get_available_dates,
    get_daily_summary,
    get_data_for_date
)

def test_database_operations():
    """Test basic database operations with sample data."""
    print("Testing database operations...")
    
    # Initialize database
    init_database()
    print("✓ Database initialized")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'date': [date.today() - timedelta(days=i) for i in range(5, 0, -1)],
        'open': [100.0, 101.0, 102.0, 103.0, 104.0],
        'high': [105.0, 106.0, 107.0, 108.0, 109.0],
        'low': [99.0, 100.0, 101.0, 102.0, 103.0],
        'close': [104.0, 105.0, 106.0, 107.0, 108.0],
        'volume': [10000, 11000, 12000, 13000, 14000]
    })
    
    # Test storing data
    rows_stored = store_ohlc_data('TEST_SYMBOL', sample_data)
    print(f"✓ Stored {rows_stored} rows for TEST_SYMBOL")
    
    # Test retrieving data
    retrieved_data = get_ohlc_data('TEST_SYMBOL')
    print(f"✓ Retrieved {len(retrieved_data)} rows for TEST_SYMBOL")
    
    # Test date functions
    available_dates = get_available_dates()
    print(f"✓ Found {len(available_dates)} available dates")
    
    if available_dates:
        # Test daily summary
        summary = get_daily_summary(available_dates[0])
        print(f"✓ Daily summary for {available_dates[0]}: {summary}")
        
        # Test data for date
        date_data = get_data_for_date(available_dates[0])
        print(f"✓ Data for {available_dates[0]}: {len(date_data)} records")
    
    print("✓ All database tests passed!")
    return True

if __name__ == "__main__":
    test_database_operations()
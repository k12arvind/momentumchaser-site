#!/usr/bin/env python3
"""
Add EMA data directly to the main scan results CSV so it shows on the main page.
This ensures EMA data is visible immediately without separate pages.
"""

import pandas as pd
import os

def add_ema_to_scan_results():
    """Add EMA columns to the main scan results."""
    
    # Read the current scan results
    scan_file = "site/data/latest.csv"
    ema_file = "site/data/latest_ema.csv"
    
    if not os.path.exists(scan_file):
        print(f"Scan file not found: {scan_file}")
        return
        
    if not os.path.exists(ema_file):
        print(f"EMA file not found: {ema_file}")
        return
    
    # Load both files
    scan_df = pd.read_csv(scan_file)
    ema_df = pd.read_csv(ema_file)
    
    print(f"Loaded scan results: {len(scan_df)} symbols")
    print(f"Loaded EMA data: {len(ema_df)} symbols")
    
    # Merge EMA data with scan results
    # Keep only essential EMA columns to avoid overwhelming the display
    ema_columns = ['symbol', 'ema_9', 'ema_50', 'above_ema_9', 'above_ema_50', 'ema_trend']
    ema_subset = ema_df[ema_columns]
    
    # Merge on symbol
    enhanced_df = scan_df.merge(ema_subset, on='symbol', how='left')
    
    # Reorder columns to put EMA data after main scan columns
    scan_cols = [col for col in scan_df.columns if col != 'symbol']
    ema_cols = ['ema_9', 'ema_50', 'above_ema_9', 'above_ema_50', 'ema_trend']
    
    new_column_order = ['symbol'] + scan_cols + ema_cols
    enhanced_df = enhanced_df[new_column_order]
    
    # Save enhanced scan results
    enhanced_df.to_csv(scan_file, index=False)
    
    print(f"Enhanced scan results saved with EMA data")
    print(f"New columns added: {ema_cols}")
    print(f"Total symbols with EMA data: {enhanced_df['ema_9'].notna().sum()}/{len(enhanced_df)}")
    
    # Show sample of enhanced data
    print("\nSample enhanced data:")
    sample_cols = ['symbol', 'score', 'close', 'ema_9', 'above_ema_9', 'ema_trend']
    available_cols = [col for col in sample_cols if col in enhanced_df.columns]
    print(enhanced_df[available_cols].head(3).to_string(index=False))

if __name__ == '__main__':
    add_ema_to_scan_results()
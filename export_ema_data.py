#!/usr/bin/env python3
"""
Export EMA data for static deployment to GitHub Pages.
Creates enhanced CSV files with EMA values for latest scan date.
"""

import pandas as pd
import sqlite3
from datetime import datetime
import os

from src.database import get_available_dates, get_data_for_date

def export_latest_with_emas():
    """Export latest scan data with EMA values for static deployment."""
    
    # Get most recent date with data
    dates = get_available_dates()
    if not dates:
        print("No data available in database")
        return
    
    latest_date = dates[0]
    print(f"Exporting EMA data for {latest_date}")
    
    # Get data with EMAs
    df = get_data_for_date(latest_date)
    if df.empty:
        print(f"No data found for {latest_date}")
        return
    
    # Add EMA trend signals
    for _, row in df.iterrows():
        close = row['close']
        ema_4 = row.get('ema_4')
        ema_9 = row.get('ema_9') 
        ema_50 = row.get('ema_50')
        ema_200 = row.get('ema_200')
        
        # Add trend signals
        df.loc[df.index == row.name, 'above_ema_9'] = close > ema_9 if ema_9 else False
        df.loc[df.index == row.name, 'above_ema_50'] = close > ema_50 if ema_50 else False
        df.loc[df.index == row.name, 'above_ema_200'] = close > ema_200 if ema_200 else False
        df.loc[df.index == row.name, 'ema_trend'] = 'bullish' if (ema_4 and ema_9 and ema_4 > ema_9) else 'bearish' if (ema_4 and ema_9 and ema_4 < ema_9) else 'neutral'
    
    # Select relevant columns for export
    export_columns = [
        'symbol', 'close', 'volume', 'high', 'low', 'open',
        'ema_4', 'ema_9', 'ema_18', 'ema_50', 'ema_200',
        'above_ema_9', 'above_ema_50', 'above_ema_200', 'ema_trend'
    ]
    
    # Filter to available columns
    available_columns = [col for col in export_columns if col in df.columns]
    df_export = df[available_columns].copy()
    
    # Round EMA values for display
    ema_cols = [col for col in df_export.columns if col.startswith('ema_')]
    for col in ema_cols:
        if df_export[col].dtype in ['float64', 'float32']:
            df_export[col] = df_export[col].round(2)
    
    # Sort by trading value descending (most active first)
    if 'traded_value' in df.columns:
        df_export = df_export.loc[df.sort_values('traded_value', ascending=False).index]
    
    # Export to site directory
    os.makedirs('site/data', exist_ok=True)
    output_file = f'site/data/ema_data_{latest_date}.csv'
    df_export.to_csv(output_file, index=False)
    
    # Also create a latest_ema.csv for easy access
    df_export.to_csv('site/data/latest_ema.csv', index=False)
    
    print(f"Exported {len(df_export)} symbols with EMA data to:")
    print(f"  - {output_file}")
    print(f"  - site/data/latest_ema.csv")
    
    # Print sample data
    print(f"\nSample EMA data:")
    sample_cols = ['symbol', 'close', 'ema_9', 'ema_50', 'above_ema_9', 'ema_trend']
    available_sample_cols = [col for col in sample_cols if col in df_export.columns]
    print(df_export[available_sample_cols].head(3).to_string(index=False))
    
    return latest_date, len(df_export)

if __name__ == '__main__':
    export_latest_with_emas()
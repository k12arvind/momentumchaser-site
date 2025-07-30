#!/usr/bin/env python3
"""
Database utility for storing and retrieving daily OHLC and volume data.
Uses SQLite for simplicity and portability.
"""

import sqlite3
import os
import pandas as pd
import numpy as np
from datetime import date, datetime
from typing import List, Dict, Any, Optional
import logging

# Database file path
DB_PATH = "data/stock_data.db"

def calculate_ema(prices: pd.Series, period: int) -> pd.Series:
    """
    Calculate Exponential Moving Average (EMA) for given prices and period.
    
    Args:
        prices: Series of prices (usually close prices)
        period: EMA period (e.g., 4, 9, 18, 50, 200)
    
    Returns:
        Series of EMA values
    """
    return prices.ewm(span=period, adjust=False).mean()

def add_ema_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add EMA columns to DataFrame based on close prices.
    
    Args:
        df: DataFrame with 'close' column
    
    Returns:
        DataFrame with additional EMA columns
    """
    if 'close' not in df.columns or df.empty:
        return df
    
    # Sort by date to ensure proper EMA calculation
    df = df.sort_values('date').copy()
    
    # Calculate EMAs
    df['ema_4'] = calculate_ema(df['close'], 4)
    df['ema_9'] = calculate_ema(df['close'], 9) 
    df['ema_18'] = calculate_ema(df['close'], 18)
    df['ema_50'] = calculate_ema(df['close'], 50)
    df['ema_200'] = calculate_ema(df['close'], 200)
    
    # Round EMA values to 2 decimal places
    ema_columns = ['ema_4', 'ema_9', 'ema_18', 'ema_50', 'ema_200']
    for col in ema_columns:
        df[col] = df[col].round(2)
    
    return df

def init_database():
    """Initialize database and create tables if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create main OHLC data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_ohlc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            traded_value REAL,
            ema_4 REAL,
            ema_9 REAL,
            ema_18 REAL,
            ema_50 REAL,
            ema_200 REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        )
    """)
    
    # Add EMA columns to existing table if they don't exist
    try:
        cursor.execute("ALTER TABLE daily_ohlc ADD COLUMN ema_4 REAL")
        cursor.execute("ALTER TABLE daily_ohlc ADD COLUMN ema_9 REAL") 
        cursor.execute("ALTER TABLE daily_ohlc ADD COLUMN ema_18 REAL")
        cursor.execute("ALTER TABLE daily_ohlc ADD COLUMN ema_50 REAL")
        cursor.execute("ALTER TABLE daily_ohlc ADD COLUMN ema_200 REAL")
    except sqlite3.OperationalError:
        # Columns already exist
        pass
    
    # Create index for faster queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol_date ON daily_ohlc(symbol, date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON daily_ohlc(date)")
    
    # Create scan results table to store daily scan metadata
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scan_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date DATE NOT NULL,
            total_symbols INTEGER,
            ranked_symbols INTEGER,
            scan_duration_seconds REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(scan_date)
        )
    """)
    
    conn.commit()
    conn.close()

def store_ohlc_data(symbol: str, df: pd.DataFrame) -> int:
    """
    Store OHLC data for a symbol. Updates existing records or inserts new ones.
    
    Args:
        symbol: Stock symbol
        df: DataFrame with columns ['date', 'open', 'high', 'low', 'close', 'volume']
    
    Returns:
        Number of records stored/updated
    """
    if df.empty:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Prepare data with traded_value calculation
        df = df.copy()
        df['symbol'] = symbol
        df['traded_value'] = df['close'] * df['volume']
        
        # Convert date to string format for SQLite
        # Handle timezone-aware dates by converting to naive dates
        if pd.api.types.is_datetime64_any_dtype(df['date']):
            # Convert timezone-aware datetime to date string
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        elif not isinstance(df['date'].iloc[0], str):
            # If it's not a string and not datetime, convert to string
            df['date'] = df['date'].astype(str)
        
        # Add EMA calculations based on close prices
        df_with_emas = add_ema_columns(df)
        
        # Select required columns including EMAs
        columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'traded_value', 
                  'ema_4', 'ema_9', 'ema_18', 'ema_50', 'ema_200']
        df_clean = df_with_emas[columns]
        
        # Use INSERT OR REPLACE to handle duplicates properly
        cursor = conn.cursor()
        rows_affected = 0
        
        for _, row in df_clean.iterrows():
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_ohlc 
                    (symbol, date, open, high, low, close, volume, traded_value, 
                     ema_4, ema_9, ema_18, ema_50, ema_200)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['symbol'], row['date'], row['open'], row['high'], row['low'],
                    row['close'], row['volume'], row['traded_value'],
                    row.get('ema_4'), row.get('ema_9'), row.get('ema_18'), 
                    row.get('ema_50'), row.get('ema_200')
                ))
                rows_affected += 1
            except Exception as e:
                logging.error(f"Error inserting row for {symbol} on {row['date']}: {e}")
                continue
        
        conn.commit()
        return rows_affected
        
    except Exception as e:
        logging.error(f"Error storing OHLC data for {symbol}: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()

def update_emas_for_symbol(symbol: str) -> bool:
    """
    Recalculate and update EMAs for a specific symbol using all historical data.
    This ensures accurate EMA calculations based on complete price history.
    
    Args:
        symbol: Stock symbol to update
    
    Returns:
        True if successful, False otherwise
    """
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Get all historical data for the symbol, ordered by date
        query = """
            SELECT date, close, id
            FROM daily_ohlc 
            WHERE symbol = ? 
            ORDER BY date
        """
        df = pd.read_sql_query(query, conn, params=[symbol])
        
        if df.empty:
            return False
        
        # Calculate EMAs for the complete dataset
        df['ema_4'] = calculate_ema(df['close'], 4)
        df['ema_9'] = calculate_ema(df['close'], 9)
        df['ema_18'] = calculate_ema(df['close'], 18)
        df['ema_50'] = calculate_ema(df['close'], 50)
        df['ema_200'] = calculate_ema(df['close'], 200)
        
        # Round EMA values
        ema_columns = ['ema_4', 'ema_9', 'ema_18', 'ema_50', 'ema_200']
        for col in ema_columns:
            df[col] = df[col].round(2)
        
        # Update database with calculated EMAs
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                UPDATE daily_ohlc 
                SET ema_4 = ?, ema_9 = ?, ema_18 = ?, ema_50 = ?, ema_200 = ?
                WHERE id = ?
            """, (row['ema_4'], row['ema_9'], row['ema_18'], 
                  row['ema_50'], row['ema_200'], row['id']))
        
        conn.commit()
        return True
        
    except Exception as e:
        logging.error(f"Error updating EMAs for {symbol}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def update_all_emas() -> None:
    """Update EMAs for all symbols in the database."""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Get all unique symbols
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM daily_ohlc ORDER BY symbol")
        symbols = [row[0] for row in cursor.fetchall()]
        
        logging.info(f"Updating EMAs for {len(symbols)} symbols...")
        
        success_count = 0
        for i, symbol in enumerate(symbols, 1):
            if update_emas_for_symbol(symbol):
                success_count += 1
                if i % 50 == 0:
                    logging.info(f"Progress: {i}/{len(symbols)} symbols processed")
        
        logging.info(f"EMA update completed. Success: {success_count}/{len(symbols)}")
        
    finally:
        conn.close()

def get_ohlc_data(symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Retrieve OHLC data for a symbol within date range.
    
    Args:
        symbol: Stock symbol
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format)
    
    Returns:
        DataFrame with OHLC data
    """
    conn = sqlite3.connect(DB_PATH)
    
    query = "SELECT * FROM daily_ohlc WHERE symbol = ?"
    params = [symbol]
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date"
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    finally:
        conn.close()

def get_available_dates() -> List[str]:
    """Get list of all available dates in the database."""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT date FROM daily_ohlc ORDER BY date DESC")
        dates = [row[0] for row in cursor.fetchall()]
        return dates
    finally:
        conn.close()

def get_symbols_for_date(target_date: str) -> List[str]:
    """Get list of symbols available for a specific date."""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM daily_ohlc WHERE date = ? ORDER BY symbol", [target_date])
        symbols = [row[0] for row in cursor.fetchall()]
        return symbols
    finally:
        conn.close()

def get_daily_summary(target_date: str) -> Dict[str, Any]:
    """Get summary statistics for a specific date."""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total_symbols,
                AVG(close) as avg_close,
                AVG(volume) as avg_volume,
                SUM(traded_value) as total_traded_value,
                MAX(high) as max_high,
                MIN(low) as min_low
            FROM daily_ohlc 
            WHERE date = ?
        """, [target_date])
        
        row = cursor.fetchone()
        if row:
            return {
                'date': target_date,
                'total_symbols': row[0],
                'avg_close': round(row[1] or 0, 2),
                'avg_volume': int(row[2] or 0),
                'total_traded_value_cr': round((row[3] or 0) / 1e7, 2),
                'max_high': round(row[4] or 0, 2),
                'min_low': round(row[5] or 0, 2)
            }
        return {}
    finally:
        conn.close()

def store_scan_metadata(scan_date: str, total_symbols: int, ranked_symbols: int, scan_duration: float):
    """Store metadata about a scan run."""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO scan_results 
            (scan_date, total_symbols, ranked_symbols, scan_duration_seconds)
            VALUES (?, ?, ?, ?)
        """, [scan_date, total_symbols, ranked_symbols, scan_duration])
        conn.commit()
    except Exception as e:
        logging.error(f"Error storing scan metadata: {e}")
    finally:
        conn.close()

def get_data_for_date(target_date: str, limit: Optional[int] = None) -> pd.DataFrame:
    """Get all OHLC data for a specific date."""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        query = """
            SELECT symbol, date, open, high, low, close, volume, traded_value,
                   ema_4, ema_9, ema_18, ema_50, ema_200
            FROM daily_ohlc 
            WHERE date = ? 
            ORDER BY traded_value DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        df = pd.read_sql_query(query, conn, params=[target_date])
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df['traded_value_cr'] = (df['traded_value'] / 1e7).round(2)
        return df
    finally:
        conn.close()

# Initialize database on import
init_database()
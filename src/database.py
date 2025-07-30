#!/usr/bin/env python3
"""
Database utility for storing and retrieving daily OHLC and volume data.
Uses SQLite for simplicity and portability.
"""

import sqlite3
import os
import pandas as pd
from datetime import date, datetime
from typing import List, Dict, Any, Optional
import logging

# Database file path
DB_PATH = "data/stock_data.db"

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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        )
    """)
    
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
        if not isinstance(df['date'].iloc[0], str):
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # Select only required columns
        columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'traded_value']
        df_clean = df[columns]
        
        # Use INSERT OR REPLACE to handle duplicates
        df_clean.to_sql('daily_ohlc', conn, if_exists='append', index=False, method='multi')
        
        # Remove duplicates (keeping latest)
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM daily_ohlc 
            WHERE id NOT IN (
                SELECT MAX(id) 
                FROM daily_ohlc 
                GROUP BY symbol, date
            )
        """)
        
        rows_affected = cursor.rowcount
        conn.commit()
        return rows_affected
        
    except Exception as e:
        logging.error(f"Error storing OHLC data for {symbol}: {e}")
        conn.rollback()
        return 0
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
            SELECT symbol, date, open, high, low, close, volume, traded_value
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
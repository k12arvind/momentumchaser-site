#!/usr/bin/env python3
"""
API server to serve historical OHLC data and scan results.
Provides REST endpoints for the frontend to query database.
"""

import os
import json
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from datetime import datetime, timedelta
import pandas as pd

from database import (
    get_available_dates,
    get_symbols_for_date,
    get_daily_summary,
    get_data_for_date,
    get_ohlc_data
)

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access

@app.route('/api/health')
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})

@app.route('/api/dates')
def get_dates():
    """Get list of available dates."""
    try:
        dates = get_available_dates()
        return jsonify({
            'dates': dates,
            'count': len(dates)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/summary/<date>')
def get_date_summary(date):
    """Get summary statistics for a specific date."""
    try:
        # Validate date format
        datetime.strptime(date, '%Y-%m-%d')
        
        summary = get_daily_summary(date)
        if not summary:
            return jsonify({'error': f'No data found for date {date}'}), 404
        
        return jsonify(summary)
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data/<date>')
def get_date_data(date):
    """Get all OHLC data for a specific date."""
    try:
        # Validate date format
        datetime.strptime(date, '%Y-%m-%d')
        
        # Get optional limit parameter
        limit = request.args.get('limit', type=int)
        
        df = get_data_for_date(date, limit)
        if df.empty:
            return jsonify({'error': f'No data found for date {date}'}), 404
        
        # Convert DataFrame to records
        data = df.to_dict('records')
        
        # Convert datetime objects to strings for JSON serialization
        for record in data:
            if 'date' in record and hasattr(record['date'], 'isoformat'):
                record['date'] = record['date'].isoformat()
        
        return jsonify({
            'date': date,
            'count': len(data),
            'data': data
        })
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/symbol/<symbol>')
def get_symbol_data(symbol):
    """Get historical data for a specific symbol."""
    try:
        symbol = symbol.upper()
        
        # Get optional date range parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Default to last 30 days if no dates provided
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        df = get_ohlc_data(symbol, start_date, end_date)
        if df.empty:
            return jsonify({'error': f'No data found for symbol {symbol}'}), 404
        
        # Convert DataFrame to records
        data = df.to_dict('records')
        
        # Convert datetime objects to strings for JSON serialization
        for record in data:
            if 'date' in record and hasattr(record['date'], 'isoformat'):
                record['date'] = record['date'].isoformat()
        
        return jsonify({
            'symbol': symbol,
            'start_date': start_date,
            'end_date': end_date,
            'count': len(data),
            'data': data
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/symbols/<date>')
def get_date_symbols(date):
    """Get list of symbols available for a specific date."""
    try:
        # Validate date format
        datetime.strptime(date, '%Y-%m-%d')
        
        symbols = get_symbols_for_date(date)
        return jsonify({
            'date': date,
            'symbols': symbols,
            'count': len(symbols)
        })
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/search')
def search_symbols():
    """Search for symbols by pattern."""
    try:
        query = request.args.get('q', '').upper().strip()
        if not query:
            return jsonify({'error': 'Query parameter "q" is required'}), 400
        
        date_param = request.args.get('date')
        if not date_param:
            # Use most recent date
            dates = get_available_dates()
            if not dates:
                return jsonify({'error': 'No data available'}), 404
            date_param = dates[0]
        
        all_symbols = get_symbols_for_date(date_param)
        matching_symbols = [s for s in all_symbols if query in s]
        
        return jsonify({
            'query': query,
            'date': date_param,
            'matches': matching_symbols,
            'count': len(matching_symbols)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bulk/<date>')
def get_bulk_data(date):
    """Get OHLC data for multiple symbols on a specific date."""
    try:
        # Validate date format
        datetime.strptime(date, '%Y-%m-%d')
        
        # Get symbols from query parameter (comma-separated)
        symbols_param = request.args.get('symbols', '')
        if not symbols_param:
            return jsonify({'error': 'symbols parameter is required'}), 400
        
        symbols = [s.strip().upper() for s in symbols_param.split(',')]
        
        results = {}
        for symbol in symbols:
            df = get_ohlc_data(symbol, date, date)
            if not df.empty:
                record = df.iloc[0].to_dict()
                # Convert datetime to string
                if 'date' in record and hasattr(record['date'], 'isoformat'):
                    record['date'] = record['date'].isoformat()
                results[symbol] = record
        
        return jsonify({
            'date': date,
            'requested_symbols': symbols,
            'found_count': len(results),
            'data': results
        })
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/symbol/<symbol>/last10days')
def get_symbol_last_10_days(symbol):
    """Get last 10 trading days of OHLC data for a specific symbol."""
    try:
        symbol = symbol.upper()
        
        # Get the most recent 10 trading days from database
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime('%Y-%m-%d')
        # Start from 20 days ago to ensure we get 10 trading days
        start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
        
        df = get_ohlc_data(symbol, start_date, end_date)
        if df.empty:
            return jsonify({'error': f'No data found for symbol {symbol}'}), 404
        
        # Get the last 10 records (most recent trading days)
        df_last_10 = df.tail(10)
        
        # Convert DataFrame to records
        data = df_last_10.to_dict('records')
        
        # Convert datetime objects to strings for JSON serialization
        for record in data:
            if 'date' in record and hasattr(record['date'], 'isoformat'):
                record['date'] = record['date'].isoformat()
            # Add percentage change calculation
            if len(data) > 1:
                prev_close = None
                for i, record in enumerate(data):
                    if i > 0:
                        prev_close = data[i-1]['close']
                        change = record['close'] - prev_close
                        change_pct = (change / prev_close) * 100 if prev_close else 0
                        record['change'] = round(change, 2)
                        record['change_pct'] = round(change_pct, 2)
                    else:
                        record['change'] = 0
                        record['change_pct'] = 0
        
        return jsonify({
            'symbol': symbol,
            'period': 'Last 10 trading days',
            'count': len(data),
            'data': data
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bulk-last10days')
def get_bulk_last_10_days():
    """Get last 10 days data for multiple symbols."""
    try:
        # Get symbols from query parameter (comma-separated)
        symbols_param = request.args.get('symbols', '')
        if not symbols_param:
            return jsonify({'error': 'symbols parameter is required'}), 400
        
        symbols = [s.strip().upper() for s in symbols_param.split(',')]
        
        results = {}
        for symbol in symbols:
            try:
                # Reuse the logic from single symbol endpoint
                from datetime import datetime, timedelta
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
                
                df = get_ohlc_data(symbol, start_date, end_date)
                if not df.empty:
                    df_last_10 = df.tail(10)
                    data = df_last_10.to_dict('records')
                    
                    # Convert datetime objects to strings
                    for record in data:
                        if 'date' in record and hasattr(record['date'], 'isoformat'):
                            record['date'] = record['date'].isoformat()
                    
                    results[symbol] = {
                        'count': len(data),
                        'data': data
                    }
            except Exception as e:
                results[symbol] = {'error': str(e)}
        
        return jsonify({
            'period': 'Last 10 trading days',
            'requested_symbols': symbols,
            'results': results
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Serve static files (for the frontend)
@app.route('/')
def serve_index():
    """Serve the main HTML page."""
    return send_from_directory('../site', 'index.html')

@app.route('/data/<path:filename>')
def serve_data_files(filename):
    """Serve data files."""
    return send_from_directory('../site/data', filename)

@app.route('/archive/<path:filename>')
def serve_archive_files(filename):
    """Serve archive files."""
    return send_from_directory('../site/archive', filename)

if __name__ == '__main__':
    print("Starting API server...")
    print("Available endpoints:")
    print("  GET /api/health - Health check")
    print("  GET /api/dates - List available dates")
    print("  GET /api/summary/<date> - Daily summary")
    print("  GET /api/data/<date> - All data for date")
    print("  GET /api/symbol/<symbol> - Symbol history")
    print("  GET /api/symbols/<date> - Symbols for date")
    print("  GET /api/search?q=<query> - Search symbols")
    print("  GET /api/bulk/<date>?symbols=<list> - Bulk data")
    
    app.run(host='127.0.0.1', port=8000, debug=True)
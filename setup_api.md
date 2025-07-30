# API Server Setup Guide

## Step 1: Install Required Dependencies

```bash
pip install pandas numpy kiteconnect flask flask-cors python-dotenv requests
```

## Step 2: Set Up Authentication

1. **Check if you have tokens.json:**
   ```bash
   ls -la tokens.json
   ```

2. **If tokens.json doesn't exist or is old, get fresh tokens:**
   ```bash
   python src/auth_server.py
   ```
   - Visit http://localhost:5000
   - Login with your Zerodha credentials
   - This creates tokens.json

3. **Verify authentication:**
   ```bash
   python src/check_kite_auth.py
   ```

## Step 3: Populate Database with OHLC Data

```bash
# Fetch last 10 trading days of data
python src/fetch_recent_data.py
```

This will:
- Download OHLC data for all NIFTY-500 stocks
- Store in SQLite database (data/stock_data.db)
- Take 10-15 minutes to complete

## Step 4: Start API Server

```bash
python src/api_server.py
```

The server will run on http://127.0.0.1:8000

## Step 5: Test the Setup

1. **Visit your website:** https://momentumchaser.com
2. **Look for changes:**
   - "Offline Mode" badge should disappear
   - "OHLC Data" option should become enabled
   - "Check API Server" button should show "API Connected"

## Step 6: Use New Features

- **Switch to OHLC Data view** to see raw price data
- **Click symbols** for detailed historical analysis
- **Browse dates** with full database-powered functionality

## Troubleshooting

- **API server won't start:** Check if port 8000 is free
- **Database errors:** Delete `data/stock_data.db` and re-run fetch_recent_data.py
- **Authentication issues:** Re-run auth_server.py to get fresh tokens
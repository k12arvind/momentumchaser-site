#!/usr/bin/env python3
"""
Create a static HTML page with EMA data directly embedded for reliable GitHub Pages deployment.
This ensures EMA data is visible without relying on separate CSV files.
"""

import pandas as pd
import os
from datetime import datetime

def create_static_ema_page():
    """Create static HTML page with EMA data embedded."""
    
    # Read the EMA data
    ema_file = "site/data/latest_ema.csv"
    if not os.path.exists(ema_file):
        print(f"EMA file not found: {ema_file}")
        return
    
    df = pd.read_csv(ema_file)
    print(f"Loaded {len(df)} symbols with EMA data")
    
    # Create HTML with EMA data embedded
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>MomentumChaser — EMA Analysis ({datetime.now().strftime('%Y-%m-%d')})</title>
    <style>
        body {{ font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif; margin: 24px; }}
        h1 {{ font-size: 1.6rem; margin: 0 0 12px; }}
        .summary {{ color:#555; margin-bottom:14px; }}
        table {{ border-collapse: collapse; width: 100%; font-size: 14px; margin-top: 16px; }}
        th, td {{ border: 1px solid #e5e5e5; padding: 8px; text-align: left; }}
        th {{ background:#fafafa; position: sticky; top:0; }}
        tr:nth-child(even) td {{ background: #fcfcfc; }}
        tr:hover td {{ background: #f0f9ff; }}
        .badge {{ display:inline-block; padding:2px 6px; border-radius:6px; background:#f3f4f6; }}
        footer {{ margin-top:24px; font-size:12px; color:#666; }}
        a {{ color:#2563eb; text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}
        .trend-bullish {{ color: #10b981; }}
        .trend-bearish {{ color: #ef4444; }}
        .trend-neutral {{ color: #6b7280; }}
        .above-ema {{ color: #10b981; }}
        .below-ema {{ color: #ef4444; }}
    </style>
</head>
<body>
    <h1>MomentumChaser — EMA Analysis Dashboard</h1>
    
    <div class="summary">
        <span class="badge">Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</span>
        &nbsp;|&nbsp;
        <span class="badge">Symbols: {len(df)}</span>
        &nbsp;|&nbsp;
        <span class="badge">EMA Analysis: 4/9/18/50/200-day</span>
        &nbsp;|&nbsp;
        <a href="https://github.com/k12arvind/momentumchaser-site" target="_blank">Source</a>
    </div>
    
    <div style="margin-bottom: 16px; padding: 12px; background: #f0f9ff; border-radius: 6px;">
        <h3 style="margin: 0 0 8px 0; color: #1e40af;">EMA Analysis Features</h3>
        <p style="margin: 0; font-size: 13px;">
            <strong>Historical Data:</strong> 191+ trading days &nbsp;|&nbsp;
            <strong>EMAs:</strong> 4, 9, 18, 50, 200-day exponential moving averages &nbsp;|&nbsp;
            <strong>Signals:</strong> Above/below EMA levels, trend analysis &nbsp;|&nbsp;
            <strong>Sorted:</strong> By trading volume (most active first)
        </p>
    </div>
    
    <table>
        <thead>
            <tr>
                <th>Symbol</th>
                <th>Close</th>
                <th>Volume</th>
                <th>EMA 9</th>
                <th>EMA 50</th>
                <th>Above EMA 9</th>
                <th>Above EMA 50</th>
                <th>Trend</th>
            </tr>
        </thead>
        <tbody>"""
    
    # Add top 100 rows (to keep page size manageable)
    for i, row in df.head(100).iterrows():
        symbol = row['symbol']
        close = row['close']
        volume = row['volume'] 
        ema_9 = row.get('ema_9', 0)
        ema_50 = row.get('ema_50', 0)
        above_ema_9 = row.get('above_ema_9', False)
        above_ema_50 = row.get('above_ema_50', False)
        trend = row.get('ema_trend', 'neutral')
        
        # Format values
        close_str = f"₹{close:.2f}" if pd.notna(close) else "N/A"
        volume_str = f"{int(volume/1000)}K" if pd.notna(volume) else "N/A"
        ema_9_str = f"₹{ema_9:.1f}" if pd.notna(ema_9) and ema_9 > 0 else "N/A"
        ema_50_str = f"₹{ema_50:.1f}" if pd.notna(ema_50) and ema_50 > 0 else "N/A"
        
        # EMA signals
        above_9_class = "above-ema" if above_ema_9 else "below-ema"
        above_50_class = "above-ema" if above_ema_50 else "below-ema"
        above_9_symbol = "✓" if above_ema_9 else "✗"
        above_50_symbol = "✓" if above_ema_50 else "✗"
        
        # Trend
        trend_class = f"trend-{trend}"
        trend_icon = "📈" if trend == "bullish" else "📉" if trend == "bearish" else "➡️"
        
        html_content += f"""
            <tr>
                <td><strong>{symbol}</strong></td>
                <td style="font-weight: 600;">{close_str}</td>
                <td>{volume_str}</td>
                <td>{ema_9_str}</td>
                <td>{ema_50_str}</td>
                <td class="{above_9_class}">{above_9_symbol}</td>
                <td class="{above_50_class}">{above_50_symbol}</td>
                <td class="{trend_class}">{trend_icon} {trend.title()}</td>
            </tr>"""
    
    html_content += """
        </tbody>
    </table>
    
    <footer>
        <p><strong>EMA Analysis:</strong> Exponential Moving Averages calculated from 191+ days of historical data.</p>
        <p><strong>Trend Signals:</strong> Bullish (EMA 4 > EMA 9), Bearish (EMA 4 < EMA 9), Neutral (sideways).</p>
        <p><strong>Data Source:</strong> Kite Connect API | <strong>Built with:</strong> Python, pandas, SQLite</p>
    </footer>
</body>
</html>"""
    
    # Write the HTML file
    output_file = "site/ema-analysis.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Created static EMA analysis page: {output_file}")
    print(f"Contains top 100 symbols with EMA data")
    print(f"This will be accessible at: https://momentumchaser.com/ema-analysis.html")

if __name__ == '__main__':
    create_static_ema_page()
# src/publish_site.py
import os, glob, pathlib, datetime
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "out"
SITE = ROOT / "site"
DATA_DIR = SITE / "data"
ARCHIVE = SITE / "archive"

def latest_scan_csv():
    # Find the most recent out/YYYY-MM-DD/todays_scan.csv
    days = sorted([p for p in OUT.glob("*") if p.is_dir()], reverse=True)
    for d in days:
        f = d / "todays_scan.csv"
        if f.exists():
            return d.name, f
    raise SystemExit("No todays_scan.csv found under out/YYYY-MM-DD/")

def render_html(date_str, df: pd.DataFrame) -> str:
    title = f"MomentumChaser — Daily Swing Scan ({date_str})"
    return f"""<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
body {{ font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif; margin: 24px; }}
h1 {{ font-size: 1.6rem; margin: 0 0 12px; }}
.controls {{ margin-bottom: 20px; padding: 16px; background: #f8f9fa; border-radius: 8px; }}
.control-group {{ margin-bottom: 12px; }}
.control-group label {{ display: inline-block; width: 120px; font-weight: 500; }}
.control-group input, .control-group select {{ padding: 6px 10px; border: 1px solid #ddd; border-radius: 4px; }}
.control-group button {{ padding: 6px 12px; background: #2563eb; color: white; border: none; border-radius: 4px; cursor: pointer; }}
.control-group button:hover {{ background: #1d4ed8; }}
.summary {{ color:#555; margin-bottom:14px; }}
.data-summary {{ margin-bottom: 16px; padding: 12px; background: #f0f9ff; border-radius: 6px; }}
.data-summary .stat {{ display: inline-block; margin-right: 20px; }}
.data-summary .stat-value {{ font-weight: 600; color: #1e40af; }}
table {{ border-collapse: collapse; width: 100%; font-size: 14px; margin-top: 16px; }}
th, td {{ border: 1px solid #e5e5e5; padding: 8px; text-align: left; }}
th {{ background:#fafafa; position: sticky; top:0; }}
tr:nth-child(even) td {{ background: #fcfcfc; }}
tr:hover td {{ background: #f0f9ff; }}
.badge {{ display:inline-block; padding:2px 6px; border-radius:6px; background:#f3f4f6; }}
.loading {{ text-align: center; padding: 40px; color: #666; }}
.error {{ color: #dc2626; background: #fef2f2; padding: 12px; border-radius: 6px; margin: 12px 0; }}
footer {{ margin-top:24px; font-size:12px; color:#666; }}
a {{ color:#2563eb; text-decoration:none; }}
a:hover {{ text-decoration:underline; }}
.symbol-link {{ font-weight: 500; color: #1e40af; cursor: pointer; }}
.symbol-link:hover {{ text-decoration: underline; }}
#symbolModal {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 1000; }}
.modal-content {{ position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); background: white; padding: 24px; border-radius: 8px; width: 80%; max-width: 600px; max-height: 80%; overflow-y: auto; }}
.modal-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }}
.modal-close {{ font-size: 24px; cursor: pointer; color: #666; }}
.modal-close:hover {{ color: #000; }}
</style>
<body>
  <h1>MomentumChaser — Daily Swing Scan & OHLC Database</h1>
  
  <div class="controls">
    <div class="control-group">
      <label for="dateSelect">Select Date:</label>
      <select id="dateSelect" onchange="loadDataForDate()">
        <option value="">Loading dates...</option>
      </select>
      <button onclick="refreshDates()">Refresh</button>
    </div>
    
    <div class="control-group">
      <label for="viewMode">View Mode:</label>
      <select id="viewMode" onchange="changeViewMode()">
        <option value="scan">Scan Results</option>
        <option value="ohlc">OHLC Data</option>
      </select>
    </div>
    
    <div class="control-group">
      <label for="symbolSearch">Search Symbol:</label>
      <input type="text" id="symbolSearch" placeholder="Enter symbol..." onkeyup="searchSymbols()" />
      <button onclick="clearSearch()">Clear</button>
    </div>
  </div>
  
  <div id="dataSummary" class="data-summary" style="display:none;">
    <!-- Summary stats will be loaded here -->
  </div>
  
  <div class="summary">
    <span class="badge" id="currentDate">Updated: {date_str}</span>
    &nbsp;|&nbsp;
    <a href="data/latest.csv" download>Download CSV</a>
    &nbsp;|&nbsp;
    <a href="archive/">Archive</a>
    &nbsp;|&nbsp;
    <a href="javascript:void(0)" onclick="startApiServer()">Start API Server</a>
  </div>
  
  <div id="loadingIndicator" class="loading" style="display:none;">
    Loading data...
  </div>
  
  <div id="errorMessage" class="error" style="display:none;">
    <!-- Error messages will appear here -->
  </div>
  
  <div id="dataContainer">
    <!-- Data table will be loaded here -->
  </div>
  
  <!-- Symbol Detail Modal -->
  <div id="symbolModal">
    <div class="modal-content">
      <div class="modal-header">
        <h3 id="modalSymbolName">Symbol Details</h3>
        <span class="modal-close" onclick="closeSymbolModal()">&times;</span>
      </div>
      <div id="modalContent">
        <!-- Symbol details will be loaded here -->
      </div>
    </div>
  </div>
  
  <footer>
    <p>Post‑close scan. Times are IST. Built with Kite historical data and a ranked trend+consolidation model.</p>
    <p><strong>New:</strong> OHLC data is now stored in database. Use the API server for detailed analysis.</p>
  </footer>

<script>
let currentDate = '{date_str}';
let currentViewMode = 'scan';
let apiBaseUrl = 'http://127.0.0.1:8000/api';
let allData = [];

// Initialize the page
document.addEventListener('DOMContentLoaded', function() {{
    loadAvailableDates();
    loadDefaultData();
}});

function showLoading() {{
    document.getElementById('loadingIndicator').style.display = 'block';
    document.getElementById('errorMessage').style.display = 'none';
}}

function hideLoading() {{
    document.getElementById('loadingIndicator').style.display = 'none';
}}

function showError(message) {{
    hideLoading();
    const errorDiv = document.getElementById('errorMessage');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
}}

function hideError() {{
    document.getElementById('errorMessage').style.display = 'none';
}}

async function loadAvailableDates() {{
    try {{
        const response = await fetch(`${{apiBaseUrl}}/dates`);
        if (!response.ok) {{
            throw new Error(`API Server not running. Please start it with: python src/api_server.py`);
        }}
        
        const data = await response.json();
        const select = document.getElementById('dateSelect');
        select.innerHTML = '<option value="">Select a date...</option>';
        
        data.dates.forEach(date => {{
            const option = document.createElement('option');
            option.value = date;
            option.textContent = date;
            if (date === currentDate) {{
                option.selected = true;
            }}
            select.appendChild(option);
        }});
        
        hideError();
    }} catch (error) {{
        showError(`Cannot load dates: ${{error.message}}`);
    }}
}}

async function loadDataForDate() {{
    const selectedDate = document.getElementById('dateSelect').value;
    if (!selectedDate) return;
    
    currentDate = selectedDate;
    document.getElementById('currentDate').textContent = `Updated: ${{selectedDate}}`;
    
    if (currentViewMode === 'scan') {{
        await loadScanResults(selectedDate);
    }} else {{
        await loadOHLCData(selectedDate);
    }}
    
    await loadDataSummary(selectedDate);
}}

async function loadScanResults(date) {{
    // Load from CSV files (existing functionality)
    showLoading();
    try {{
        const response = await fetch(`archive/${{date}}.csv`);
        if (!response.ok) {{
            throw new Error(`No scan results found for ${{date}}`);
        }}
        
        const csvText = await response.text();
        const table = csvToHtmlTable(csvText);
        document.getElementById('dataContainer').innerHTML = table;
        hideLoading();
    }} catch (error) {{
        showError(`Cannot load scan results: ${{error.message}}`);
    }}
}}

async function loadOHLCData(date) {{
    showLoading();
    try {{
        const response = await fetch(`${{apiBaseUrl}}/data/${{date}}`);
        if (!response.ok) {{
            throw new Error(`No OHLC data found for ${{date}}`);
        }}
        
        const result = await response.json();
        allData = result.data;
        renderOHLCTable(result.data);
        hideLoading();
    }} catch (error) {{
        showError(`Cannot load OHLC data: ${{error.message}}`);
    }}
}}

async function loadDataSummary(date) {{
    try {{
        const response = await fetch(`${{apiBaseUrl}}/summary/${{date}}`);
        if (response.ok) {{
            const summary = await response.json();
            renderDataSummary(summary);
        }}
    }} catch (error) {{
        console.log('Could not load data summary:', error);
    }}
}}

function renderDataSummary(summary) {{
    const html = `
        <div class="stat">Total Symbols: <span class="stat-value">${{summary.total_symbols}}</span></div>
        <div class="stat">Avg Close: <span class="stat-value">₹${{summary.avg_close}}</span></div>
        <div class="stat">Total Traded Value: <span class="stat-value">₹${{summary.total_traded_value_cr}} Cr</span></div>
        <div class="stat">Range: <span class="stat-value">₹${{summary.min_low}} - ₹${{summary.max_high}}</span></div>
    `;
    document.getElementById('dataSummary').innerHTML = html;
    document.getElementById('dataSummary').style.display = 'block';
}}

function renderOHLCTable(data) {{
    const headers = ['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'Traded Value (Cr)'];
    
    let html = '<table><thead><tr>';
    headers.forEach(header => {{
        html += `<th>${{header}}</th>`;
    }});
    html += '</tr></thead><tbody>';
    
    data.forEach(row => {{
        html += '<tr>';
        html += `<td><span class="symbol-link" onclick="showSymbolDetails('${{row.symbol}}')">${{row.symbol}}</span></td>`;
        html += `<td>₹${{row.open.toFixed(2)}}</td>`;
        html += `<td>₹${{row.high.toFixed(2)}}</td>`;
        html += `<td>₹${{row.low.toFixed(2)}}</td>`;
        html += `<td>₹${{row.close.toFixed(2)}}</td>`;
        html += `<td>${{row.volume.toLocaleString()}}</td>`;
        html += `<td>₹${{row.traded_value_cr}}</td>`;
        html += '</tr>';
    }});
    
    html += '</tbody></table>';
    document.getElementById('dataContainer').innerHTML = html;
}}

function csvToHtmlTable(csvText) {{
    const lines = csvText.trim().split('\\n');
    const headers = lines[0].split(',');
    
    let html = '<table><thead><tr>';
    headers.forEach(header => {{
        html += `<th>${{header.replace(/"/g, '')}}</th>`;
    }});
    html += '</tr></thead><tbody>';
    
    for (let i = 1; i < lines.length; i++) {{
        const cells = lines[i].split(',');
        html += '<tr>';
        cells.forEach((cell, index) => {{
            const cleanCell = cell.replace(/"/g, '');
            if (index === 0) {{
                // Make symbol clickable
                html += `<td><span class="symbol-link" onclick="showSymbolDetails('${{cleanCell}}')">${{cleanCell}}</span></td>`;
            }} else {{
                html += `<td>${{cleanCell}}</td>`;
            }}
        }});
        html += '</tr>';
    }}
    
    html += '</tbody></table>';
    return html;
}}

function changeViewMode() {{
    currentViewMode = document.getElementById('viewMode').value;
    loadDataForDate();
}}

function refreshDates() {{
    loadAvailableDates();
}}

function searchSymbols() {{
    const query = document.getElementById('symbolSearch').value.toUpperCase();
    if (!query) {{
        // Show all data
        if (currentViewMode === 'ohlc') {{
            renderOHLCTable(allData);
        }}
        return;
    }}
    
    if (currentViewMode === 'ohlc') {{
        const filteredData = allData.filter(row => row.symbol.includes(query));
        renderOHLCTable(filteredData);
    }}
}}

function clearSearch() {{
    document.getElementById('symbolSearch').value = '';
    searchSymbols();
}}

async function showSymbolDetails(symbol) {{
    const modal = document.getElementById('symbolModal');
    const modalContent = document.getElementById('modalContent');
    const modalTitle = document.getElementById('modalSymbolName');
    
    modalTitle.textContent = `${{symbol}} - Historical Data`;
    modalContent.innerHTML = '<p>Loading symbol details...</p>';
    modal.style.display = 'block';
    
    try {{
        const response = await fetch(`${{apiBaseUrl}}/symbol/${{symbol}}?start_date=2024-01-01`);
        if (!response.ok) {{
            throw new Error('Failed to load symbol data');
        }}
        
        const result = await response.json();
        renderSymbolChart(result.data);
    }} catch (error) {{
        modalContent.innerHTML = `<p class="error">Error loading data: ${{error.message}}</p>`;
    }}
}}

function renderSymbolChart(data) {{
    const modalContent = document.getElementById('modalContent');
    
    // Simple table view for now - can be enhanced with charts later
    let html = '<table style="font-size: 12px;"><thead><tr>';
    html += '<th>Date</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th>';
    html += '</tr></thead><tbody>';
    
    data.slice(-30).forEach(row => {{  // Show last 30 records
        html += '<tr>';
        html += `<td>${{row.date.split('T')[0]}}</td>`;
        html += `<td>₹${{row.open.toFixed(2)}}</td>`;
        html += `<td>₹${{row.high.toFixed(2)}}</td>`;
        html += `<td>₹${{row.low.toFixed(2)}}</td>`;
        html += `<td>₹${{row.close.toFixed(2)}}</td>`;
        html += `<td>${{row.volume.toLocaleString()}}</td>`;
        html += '</tr>';
    }});
    
    html += '</tbody></table>';
    modalContent.innerHTML = `<p>Last 30 trading days:</p>${{html}}`;
}}

function closeSymbolModal() {{
    document.getElementById('symbolModal').style.display = 'none';
}}

function loadDefaultData() {{
    // Load the default data from the current page
    const defaultTable = `{df.to_html(index=False, escape=False, classes="").replace('class=""', '')}`;
    document.getElementById('dataContainer').innerHTML = defaultTable;
    
    // Make symbols clickable in the default table
    const symbolCells = document.querySelectorAll('#dataContainer table tbody tr td:first-child');
    symbolCells.forEach(cell => {{
        const symbol = cell.textContent.trim();
        cell.innerHTML = `<span class="symbol-link" onclick="showSymbolDetails('${{symbol}}')">${{symbol}}</span>`;
    }});
}}

function startApiServer() {{
    alert('To start the API server, run:\\n\\npython src/api_server.py\\n\\nThen refresh this page to use the interactive features.');
}}

// Close modal when clicking outside
window.onclick = function(event) {{
    const modal = document.getElementById('symbolModal');
    if (event.target === modal) {{
        modal.style.display = 'none';
    }}
}}
</script>
</body>
</html>
"""

def main():
    date_str, csv_path = latest_scan_csv()
    SITE.mkdir(exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE.mkdir(parents=True, exist_ok=True)

    # Load and sort by score desc (if present)
    df = pd.read_csv(csv_path)
    if "score" in df.columns:
        df = df.sort_values(["score","box_span_pct"], ascending=[False, True])

    # write index.html
    html = render_html(date_str, df)
    (SITE / "index.html").write_text(html, encoding="utf-8")

    # copy CSVs
    df.to_csv(DATA_DIR / "latest.csv", index=False)
    # also save an archived CSV
    df.to_csv(ARCHIVE / f"{date_str}.csv", index=False)

    print(f"Wrote site/index.html and data/latest.csv for {date_str}")

if __name__ == "__main__":
    main()

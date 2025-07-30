# src/auth_server.py
import os, json
from flask import Flask, redirect, request
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("KITE_API_SECRET") or input("Paste your KITE API SECRET (won't be saved): ").strip()

app = Flask(__name__)
kite = KiteConnect(api_key=API_KEY)

@app.route("/")
def index():
    return redirect(kite.login_url())  # Zerodha login page (Kite flow)

@app.route("/auth")  # <-- must match your app's Redirect URL
def auth():
    req_token = request.args["request_token"]
    data = kite.generate_session(req_token, api_secret=API_SECRET)
    # Save only serializable, useful fields
    tokens = {
        "api_key": API_KEY,
        "user_id": data.get("user_id"),
        "access_token": data["access_token"],
        "public_token": data.get("public_token"),
        # store login_time as ISO string if present
        "login_time": data.get("login_time").isoformat() if data.get("login_time") else None,
    }
    with open("tokens.json", "w") as f:
        json.dump(tokens, f, indent=2)
    return "OK. tokens.json saved. You can close this tab."

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)


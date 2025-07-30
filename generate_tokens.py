#!/usr/bin/env python3
"""
Manual token generation script for Kite Connect
"""
import json
from kiteconnect import KiteConnect

# Your credentials
API_KEY = "8lhoc7l41fzovybm"
API_SECRET = "g9ocxtoy40dq5013ak1slydhulg30s94"

# Initialize KiteConnect
kite = KiteConnect(api_key=API_KEY)

print("=" * 60)
print("KITE CONNECT TOKEN GENERATION")
print("=" * 60)
print(f"1. Open this URL in your browser:")
print(f"   {kite.login_url()}")
print()
print("2. Login with your Zerodha credentials")
print("3. After login, you'll be redirected to a URL like:")
print("   https://127.0.0.1:5000/auth?request_token=XXXXXX&action=login&status=success")
print()
print("4. Copy the 'request_token' value from that URL and paste it below")
print("=" * 60)

request_token = input("Enter request_token: ").strip()

try:
    # Generate session
    data = kite.generate_session(request_token, api_secret=API_SECRET)
    
    # Save tokens
    tokens = {
        "api_key": API_KEY,
        "user_id": data.get("user_id"),
        "access_token": data["access_token"],
        "public_token": data.get("public_token"),
        "login_time": data.get("login_time").isoformat() if data.get("login_time") else None,
    }
    
    with open("tokens.json", "w") as f:
        json.dump(tokens, f, indent=2)
    
    print(f"\n✅ SUCCESS! tokens.json created")
    print(f"User ID: {data.get('user_id')}")
    print(f"Access Token: {data['access_token'][:20]}...")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("Please check your request_token and try again")
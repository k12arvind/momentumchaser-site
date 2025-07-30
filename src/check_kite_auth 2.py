#!/usr/bin/env python3
"""
Check Kite Connect authentication status and basic connectivity.
"""

import os
import json
from kite_client import get_kite, instruments_nse_eq

def check_authentication():
    """Check if Kite Connect authentication is working."""
    print("Checking Kite Connect authentication...")
    
    # Check if tokens.json exists
    if not os.path.exists("tokens.json"):
        print("❌ tokens.json not found")
        print("💡 Run: python src/auth_server.py")
        print("💡 Then visit http://localhost:5000 to complete login")
        return False
    
    # Check if .env file exists
    if not os.path.exists(".env"):
        print("❌ .env file not found")
        print("💡 Create .env file with KITE_API_KEY and KITE_API_SECRET")
        return False
    
    try:
        # Try to create Kite client
        kite = get_kite()
        print("✓ Kite client created successfully")
        
        # Test API call - get profile
        profile = kite.profile()
        print(f"✓ Authentication successful")
        print(f"  User ID: {profile.get('user_id')}")
        print(f"  User Name: {profile.get('user_name')}")
        print(f"  Email: {profile.get('email')}")
        
        # Test instruments data
        print("\nTesting instruments data...")
        inst_df = instruments_nse_eq(kite)
        print(f"✓ Retrieved {len(inst_df)} NSE EQ instruments")
        
        # Show sample instruments
        print("\nSample instruments:")
        sample = inst_df.head(3)[['tradingsymbol', 'name', 'instrument_token']]
        for _, row in sample.iterrows():
            print(f"  {row['tradingsymbol']}: {row['name']} (Token: {row['instrument_token']})")
        
        print("\n✅ Kite Connect authentication is working correctly!")
        return True
        
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        print("\n💡 Troubleshooting steps:")
        print("1. Check if tokens.json contains valid access_token")
        print("2. Tokens expire daily - you may need to re-authenticate")
        print("3. Run: python src/auth_server.py")
        print("4. Visit http://localhost:5000 to get fresh tokens")
        return False

def check_tokens_file():
    """Check the contents of tokens.json file."""
    try:
        with open("tokens.json", "r") as f:
            tokens = json.load(f)
        
        print("\nTokens file contents:")
        print(f"  API Key: {tokens.get('api_key', 'Not found')}")
        print(f"  User ID: {tokens.get('user_id', 'Not found')}")
        print(f"  Access Token: {'Present' if tokens.get('access_token') else 'Missing'}")
        print(f"  Login Time: {tokens.get('login_time', 'Not found')}")
        
        return True
    except FileNotFoundError:
        print("❌ tokens.json file not found")
        return False
    except json.JSONDecodeError:
        print("❌ tokens.json file is corrupted")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("KITE CONNECT AUTHENTICATION CHECK")
    print("=" * 50)
    
    check_tokens_file()
    success = check_authentication()
    
    if success:
        print("\n🚀 Ready to fetch data!")
        print("Run: python src/fetch_recent_data.py")
    else:
        print("\n⚠️  Please fix authentication issues first")
    
    print("=" * 50)
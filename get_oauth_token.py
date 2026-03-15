"""
Run once locally to obtain a Google OAuth refresh token.
Usage:
  pip install google-auth-oauthlib
  python get_oauth_token.py

Set these env vars first (or edit the constants below):
  GOOGLE_OAUTH_CLIENT_ID
  GOOGLE_OAUTH_CLIENT_SECRET

Then copy the printed refresh_token into GitHub Secrets.
"""

import os
import json
from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_ID     = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

client_config = {
    "installed": {
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
        "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
        "token_uri":     "https://oauth2.googleapis.com/token",
    }
}

flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
creds = flow.run_local_server(port=0)

print("\n" + "="*60)
print("Copy these 3 values into GitHub Secrets:")
print("="*60)
print(f"GOOGLE_OAUTH_CLIENT_ID     = {CLIENT_ID}")
print(f"GOOGLE_OAUTH_CLIENT_SECRET = {CLIENT_SECRET}")
print(f"GOOGLE_OAUTH_REFRESH_TOKEN = {creds.refresh_token}")
print("="*60 + "\n")

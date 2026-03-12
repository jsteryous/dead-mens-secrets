#!/usr/bin/env python3
"""
Run this ONCE locally to get your YouTube refresh token.
Requests both upload AND analytics read permissions.
Then add YOUTUBE_REFRESH_TOKEN to Railway environment variables.
"""
import requests
import webbrowser

CLIENT_ID     = input("Paste your YouTube OAuth Client ID: ").strip()
CLIENT_SECRET = input("Paste your YouTube OAuth Client Secret: ").strip()

# Request BOTH scopes — upload + analytics read
SCOPES = " ".join([
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
])

auth_url = (
    "https://accounts.google.com/o/oauth2/auth"
    f"?client_id={CLIENT_ID}"
    "&redirect_uri=urn:ietf:wg:oauth:2.0:oob"
    "&response_type=code"
    f"&scope={requests.utils.quote(SCOPES)}"
    "&access_type=offline"
    "&prompt=consent"
)

print("\nOpening browser. Authorize ALL requested permissions.")
webbrowser.open(auth_url)
code = input("\nPaste the authorization code here: ").strip()

response = requests.post(
    "https://oauth2.googleapis.com/token",
    data={
        "code":          code,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri":  "urn:ietf:wg:oauth:2.0:oob",
        "grant_type":    "authorization_code"
    }
)

tokens = response.json()
print(f"\n{'='*60}")
print(f"YOUTUBE_REFRESH_TOKEN = {tokens.get('refresh_token', 'ERROR — try again')}")
print(f"{'='*60}")
print("\nAdd this to Railway as YOUTUBE_REFRESH_TOKEN environment variable.")
print("This token has upload + analytics read permissions.")
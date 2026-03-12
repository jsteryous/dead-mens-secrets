# Dead Men's Secrets — YouTube Shorts Agent
## Setup Guide

---

## What This Builds
A fully autonomous YouTube Shorts channel that posts one video daily.
Pipeline: Topic queue → Claude writes script → ElevenLabs reads it in your voice → FFmpeg assembles video → YouTube posts it automatically.
Your involvement after setup: zero.

---

## STEP 1 — Get Your YouTube Refresh Token

This is a one-time step to authorize the agent to post to your channel.

1. On your computer, install Python if you don't have it
2. Run: `pip install requests`
3. Run: `python get_refresh_token.py`
4. Paste your YouTube OAuth Client ID and Secret when prompted
5. A browser opens — log in with the Google account that owns your YouTube channel
6. Authorize the app
7. Paste the code back into the terminal
8. Copy the YOUTUBE_REFRESH_TOKEN it gives you — save it

---

## STEP 2 — Deploy to Railway

1. Push all these files to a new GitHub repo named `dead-mens-secrets`
2. Go to railway.app → New Project → Deploy from GitHub repo
3. Select your new repo
4. Railway detects the Dockerfile automatically

---

## STEP 3 — Add Environment Variables in Railway

In your Railway service → Variables tab, add these:

| Variable | Value |
|---|---|
| ANTHROPIC_API_KEY | your_anthropic_api_key_here |
| ELEVENLABS_API_KEY | your key from elevenlabs.io |
| YOUTUBE_CLIENT_ID | from Google Cloud Console |
| YOUTUBE_CLIENT_SECRET | from Google Cloud Console |
| YOUTUBE_REFRESH_TOKEN | from Step 1 above |

---

## STEP 4 — Set the Cron Schedule

Railway.json already sets this to run at 9am UTC daily (5am EST).
To change the time, edit the cronSchedule in railway.json:
- `"0 9 * * *"` = 9am UTC every day
- `"0 14 * * *"` = 2pm UTC (10am EST)

---

## STEP 5 — Test It

In Railway → your service → click "Deploy" to trigger a manual run.
Watch the logs — you'll see each step complete in real time.
If successful, check your YouTube channel — the Short will be live.

---

## STEP 6 — Add More Topics

Open topics.json and add new topics to the list anytime.
The agent picks randomly from unused topics, then resets when all are used.
Aim to keep 60+ topics in the queue (2 months of content buffer).

---

## Cost Breakdown
- ElevenLabs Starter: $5/month (commercial rights + 30k chars)
- Claude Haiku API: ~$3/month at 1 video/day
- Railway cron job: ~$0 (minimal compute, runs for ~2 min/day)
- YouTube API: free
- **Total: ~$8/month**

---

## Troubleshooting

**FFmpeg error:** Check Railway logs — usually a font path issue
**ElevenLabs 401:** Check your API key in Railway variables
**YouTube 403:** Your refresh token may have expired — rerun get_refresh_token.py
**No video on channel:** Check if YouTube channel is verified (required for uploads)

---

## Adding Your YouTube Channel

Make sure your YouTube channel is verified:
1. Go to youtube.com/verify
2. Enter your phone number
3. This unlocks the ability to upload videos via API


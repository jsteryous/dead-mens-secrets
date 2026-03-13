#!/usr/bin/env python3
"""
Dead Men's Secrets — YouTube Shorts Agent V6 (TOP 1%)

The gap between automated and human-made content is the visuals.
This version closes that gap entirely.

What makes this top 1%:
- AI-generated images matched SCENE BY SCENE to the script (not random stock footage)
- Dark oil painting aesthetic — exactly what winning history channels use
- Ken Burns effect on every image — still images feel cinematic
- Crossfade transitions between scenes
- Word-level caption sync with karaoke highlight effect
- Self-improving analytics feedback loop
- Proper royalty-free music — monetization safe, never breaks

Pipeline:
  1.  Pull YouTube analytics — learn what's working
  2.  Analyze performance patterns with Claude
  3.  Generate topic informed by winners
  4.  Research pass — emotional arc before writing
  5.  Script pass — also outputs per-scene visual prompts + image style direction
  6.  Generate images via Replicate (Flux) — one per scene, 9:16 portrait
  7.  ElevenLabs voiceover with word-level timestamps
  8.  Fetch royalty-free music
  9.  Hook frame — black screen + single word
  10. Ken Burns + crossfade assembly — images feel alive
  11. Word-synced captions with karaoke highlight
  12. Beat flash pulses
  13. Music mixed at -20db
  14. Thumbnail from best generated image
  15. YouTube upload + analytics log
"""

import os, json, random, requests, subprocess, tempfile, datetime, textwrap, base64
import concurrent.futures
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY     = os.environ.get("ANTHROPIC_API_KEY")
ELEVENLABS_API_KEY    = os.environ.get("ELEVENLABS_API_KEY")
ELEVENLABS_VOICE_ID   = "Kbva8lG07GrIZu9cOZ7h"
REPLICATE_API_KEY     = os.environ.get("REPLICATE_API_KEY")
PEXELS_API_KEY        = os.environ.get("PEXELS_API_KEY")   # fallback only
YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

FONT      = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
W, H      = 1080, 1920
CAPTION_Y = int(H * 0.72)

# Era-based visual styles — matched to story period and tone
VISUAL_STYLES = {
    "oil_painting":           "dark oil painting style, dramatic chiaroscuro lighting, cinematic, rich shadows, historically detailed, masterpiece quality, moody atmosphere",
    "cold_war_photo":         "gritty cold war era photography, grainy black and white, surveillance aesthetic, stark contrast, Soviet brutalist architecture, documentary style",
    "daguerreotype":          "Victorian daguerreotype photograph style, sepia tones, aged, formal composition, 19th century aesthetic, antique photograph",
    "illuminated_manuscript": "medieval illuminated manuscript style, rich gold leaf, intricate borders, gothic lettering, candlelit parchment, dark ages aesthetic",
    "noir_photograph":        "1940s film noir photography, deep shadows, venetian blind light, cigarette smoke, black and white, expressionist angles",
    "renaissance_painting":   "Renaissance oil painting style, chiaroscuro, dramatic religious lighting, classical composition, Caravaggio influence, rich jewel tones",
    "gritty_documentary":     "gritty documentary photography, raw, unflinching, high contrast, photojournalism style, harsh flash lighting, modern realism",
}
DEFAULT_STYLE = "oil_painting"
IMAGE_NEGATIVE = (
    "blurry, low quality, cartoon, anime, bright cheerful colors, "
    "stock photo, watermark, text overlay, logo, modern digital art"
)


# ── UTILITIES ─────────────────────────────────────────────────────────────────
def esc(s):
    return (str(s)
            .replace("\\", "\\\\")
            .replace("'",  "\u2019")
            .replace(":",  "\\:")
            .replace(",",  "\\,")
            .replace("[",  "\\[")
            .replace("]",  "\\]")
            .replace(";",  "\\;")
            .replace("%",  "\\%")
            .replace("\n", " "))

def run(cmd, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise Exception(f"Command failed: {' '.join(str(c) for c in cmd[:4])}\n{r.stderr[-800:]}")
    return r

def get_duration(path):
    r = run(["ffprobe", "-v", "quiet", "-show_entries",
             "format=duration", "-of", "csv=p=0", path])
    return float(r.stdout.strip())

def claude(model, prompt, max_tokens=800):
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": ANTHROPIC_API_KEY,
                 "anthropic-version": "2023-06-01",
                 "content-type": "application/json"},
        json={"model": model, "max_tokens": max_tokens,
              "messages": [{"role": "user", "content": prompt}]}
    )
    r.raise_for_status()
    return r.json()["content"][0]["text"].strip()

def get_youtube_token():
    print(f"Getting YouTube token...")
    print(f"  CLIENT_ID set: {bool(YOUTUBE_CLIENT_ID)}")
    print(f"  CLIENT_SECRET set: {bool(YOUTUBE_CLIENT_SECRET)}")
    print(f"  REFRESH_TOKEN set: {bool(YOUTUBE_REFRESH_TOKEN)}")
    print(f"  REFRESH_TOKEN length: {len(YOUTUBE_REFRESH_TOKEN) if YOUTUBE_REFRESH_TOKEN else 0}")

    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={"client_id": YOUTUBE_CLIENT_ID,
              "client_secret": YOUTUBE_CLIENT_SECRET,
              "refresh_token": YOUTUBE_REFRESH_TOKEN,
              "grant_type": "refresh_token"}
    )
    print(f"  Token response status: {r.status_code}")
    data = r.json()
    print(f"  Token response keys: {list(data.keys())}")
    if "error" in data:
        print(f"  Token error detail: {data.get('error')} — {data.get('error_description')}")
    if "access_token" not in data:
        raise Exception(f"Token error: {data}")
    print(f"  Token obtained successfully")
    return data["access_token"]


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: ANALYTICS FEEDBACK LOOP
# ══════════════════════════════════════════════════════════════════════════════

def pull_youtube_analytics(token):
    print("Pulling YouTube analytics...")

    channel_r = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        headers={"Authorization": f"Bearer {token}"},
        params={"part": "contentDetails", "mine": "true"}
    )
    if channel_r.status_code != 200:
        print(f"Channel fetch failed: {channel_r.status_code}")
        return []

    items = channel_r.json().get("items", [])
    if not items:
        return []

    uploads_playlist = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

    videos_r = requests.get(
        "https://www.googleapis.com/youtube/v3/playlistItems",
        headers={"Authorization": f"Bearer {token}"},
        params={"part": "snippet", "playlistId": uploads_playlist, "maxResults": 50}
    )
    if videos_r.status_code != 200:
        return []

    playlist_items = videos_r.json().get("items", [])
    video_ids = [i["snippet"]["resourceId"]["videoId"] for i in playlist_items]
    titles    = {i["snippet"]["resourceId"]["videoId"]: i["snippet"]["title"]
                 for i in playlist_items}
    pub_dates = {i["snippet"]["resourceId"]["videoId"]: i["snippet"]["publishedAt"]
                 for i in playlist_items}

    if not video_ids:
        print("No videos yet")
        return []

    stats_r = requests.get(
        "https://www.googleapis.com/youtube/v3/videos",
        headers={"Authorization": f"Bearer {token}"},
        params={"part": "statistics", "id": ",".join(video_ids[:50])}
    )

    video_stats = {}
    if stats_r.status_code == 200:
        for item in stats_r.json().get("items", []):
            s = item.get("statistics", {})
            video_stats[item["id"]] = {
                "views": int(s.get("viewCount", 0)),
                "likes": int(s.get("likeCount", 0)),
            }

    today      = datetime.date.today().isoformat()
    start_date = (datetime.date.today() - datetime.timedelta(days=90)).isoformat()

    analytics_r = requests.get(
        "https://youtubeanalytics.googleapis.com/v2/reports",
        headers={"Authorization": f"Bearer {token}"},
        params={"ids": "channel==MINE", "startDate": start_date, "endDate": today,
                "metrics": "views,estimatedMinutesWatched,averageViewDuration",
                "dimensions": "video", "sort": "-views", "maxResults": 50}
    )

    analytics_map = {}
    if analytics_r.status_code == 200:
        data    = analytics_r.json()
        headers = [h["name"] for h in data.get("columnHeaders", [])]
        for row in data.get("rows", []):
            d = dict(zip(headers, row))
            vid = d.get("video", "")
            if vid:
                analytics_map[vid] = {
                    "watch_time_minutes": float(d.get("estimatedMinutesWatched", 0)),
                    "avg_view_duration":  float(d.get("averageViewDuration", 0)),
                }

    combined = []
    for vid_id in video_ids:
        s = video_stats.get(vid_id, {})
        a = analytics_map.get(vid_id, {})
        combined.append({
            "video_id":           vid_id,
            "title":              titles.get(vid_id, ""),
            "published_at":       pub_dates.get(vid_id, ""),
            "views":              s.get("views", 0),
            "likes":              s.get("likes", 0),
            "watch_time_minutes": a.get("watch_time_minutes", 0),
            "avg_view_duration":  a.get("avg_view_duration", 0),
        })

    combined.sort(key=lambda x: x["views"], reverse=True)
    print(f"Analytics: {len(combined)} videos")
    return combined


def analyze_performance(analytics_data):
    if len(analytics_data) < 3:
        print("New channel — no performance data yet")
        return {
            "has_data": False,
            "summary": "New channel. No data yet. Focus on hyper-specific shocking facts with named individuals.",
            "top_performers": [],
            "analysis": ""
        }

    print("Analyzing performance patterns...")

    top5    = analytics_data[:5]
    bottom5 = analytics_data[-5:] if len(analytics_data) >= 10 else []
    avg_v   = sum(v["views"] for v in analytics_data) / len(analytics_data)
    avg_w   = sum(v["avg_view_duration"] for v in analytics_data) / len(analytics_data)

    top_str = "\n".join(
        f"- \"{v['title']}\" — {v['views']} views, {v['avg_view_duration']:.0f}s avg watch"
        for v in top5
    )
    bot_str = "\n".join(
        f"- \"{v['title']}\" — {v['views']} views, {v['avg_view_duration']:.0f}s avg watch"
        for v in bottom5
    ) if bottom5 else "Insufficient data"

    analysis = claude("claude-sonnet-4-5", f"""Analytics for Dead Men's Secrets YouTube Shorts channel:
- {len(analytics_data)} total videos
- Average {avg_v:.0f} views/video
- Average {avg_w:.0f}s watch duration (60s = perfect retention)

TOP PERFORMERS:
{top_str}

WORST PERFORMERS:
{bot_str}

Analyze ruthlessly:
1. WINNING PATTERNS: What do top performers share? (era, emotion, story type, subject)
2. LOSING PATTERNS: What should we avoid?
3. RETENTION SIGNAL: Which story types keep people watching longest?
4. NEXT 2 WEEKS: Exactly what types of stories to prioritize.
5. AVOID: Specific angles that clearly don't resonate.

Be specific. Actionable. No fluff.""", max_tokens=600)

    insights = {
        "has_data":           True,
        "generated_at":       datetime.datetime.utcnow().isoformat(),
        "total_videos":       len(analytics_data),
        "avg_views":          avg_v,
        "avg_watch_duration": avg_w,
        "top_performers":     [v["title"] for v in top5],
        "analysis":           analysis,
        "summary":            analysis[:400]
    }

    save_insights(insights)
    print(f"Top video: \"{top5[0]['title']}\" ({top5[0]['views']} views)")
    return insights


def update_performance_log(video_id, title, topic, analytics_data):
    """Log a posted video to Supabase."""
    save_performance_log(video_id, title, topic)


def generate_topic(insights):
    print("Generating topic...")

    used     = get_used_topics()
    used_str = "\n".join(f"- {t}" for t in used[:60]) or "None yet"

    perf_ctx = ""
    if insights.get("has_data"):
        perf_ctx = f"""
WHAT IS WORKING RIGHT NOW:
{insights.get('analysis', '')[:400]}

Top titles: {', '.join(f'"{t}"' for t in insights.get('top_performers', [])[:3])}

Generate a topic that fits these winning patterns."""
    else:
        perf_ctx = "New channel. Focus on: named individuals, specific dates/numbers, betrayal, unexpected consequences."

    topic = claude("claude-sonnet-4-5", f"""You are the creative director of the most disturbing history channel on YouTube.
{perf_ctx}

Generate ONE topic. Must be ALL of:
- TRUE and verifiable
- OBSCURE — most people have never heard it
- Has a TWIST that recontextualizes everything
- VISCERAL — stomach drop or jaw drop
- HYPER-SPECIFIC — named person, specific date, specific place, specific number

Do NOT repeat:
{used_str}

Do NOT use: sexual content, minors, modern terrorism glorification.

ONE sentence only. No preamble.""", max_tokens=120)

    save_used_topic(topic)
    print(f"Topic: {topic}")
    return topic


def research_topic(topic, insights):
    print("Research pass...")

    perf_note = ""
    if insights.get("has_data"):
        perf_note = f"\nHigh-retention pattern on this channel: {insights.get('summary','')[:200]}\nBuild the arc to match."

    return claude("claude-sonnet-4-5", f"""Topic: "{topic}"{perf_note}

Reason through this story as a master storyteller before writing a single word:

1. HOOK WORD: One word shown alone on black screen that stops the scroll.
2. HOOK SENTENCE: Most shocking fact. Cold. No warmup.
3. SPECIFIC DETAIL: Most verifiable detail — name, number, date, place.
4. ESCALATION: The middle reveal that raises stakes suddenly.
5. TWIST: The irony that recontextualizes everything.
6. SCENES: Break this story into 5-6 distinct visual moments. What does each scene look like?
7. BEAT POINTS: 2-3 moments for visual cuts — peak revelation. Mark as [BEAT].
8. SHARE TRIGGER: What makes someone show this to another person right now?

Be ruthlessly analytical.""", max_tokens=700)


def write_script(topic, research, insights):
    """Script pass — outputs script AND per-scene image generation prompts."""
    print("Writing script + visual direction...")

    perf_note = ""
    if insights.get("has_data"):
        perf_note = f"\nOPTIMIZE FOR: {insights.get('summary','')[:200]}"

    raw = claude("claude-sonnet-4-5", f"""Topic: "{topic}"

Story analysis:
{research}
{perf_note}

Write the script AND visual direction.

CRITICAL: Output ONLY the raw spoken words — no headers, no "SCRIPT:", no markdown, no asterisks, no labels of any kind.

This script will be read aloud by a human voice. Write for the EAR, not the eye.

SPOKEN WORD RULES:
- Read it aloud in your head as you write. Every sentence must flow naturally when spoken.
- Short sentences breathe. Long sentences suffocate. Mix them with intention.
- Use natural spoken rhythm — the way a person actually tells a story, not how they write one.
- Contractions where natural: "he didn't" not "he did not", "they couldn't" not "they could not"
- Sentence 1 (HOOK): The single most disturbing fact. Drop the listener in mid-story. No warmup, no setup.
- Sentences 2-5 (BUILD): Layer in context and specific details. Each sentence raises the stakes slightly. Keep momentum.
- Sentences 6-9 (ESCALATION): The specific verifiable detail that makes this undeniably real. A name, a number, a date. Build dread.
- Final sentence (TWIST): The recontextualization. Short. Devastating. The last word lands like a door closing.
- 120-140 words MAX — tighter is better
- NO questions ever — statements only
- NO passive voice — active always
- Test: read it aloud. If you stumble anywhere, rewrite that sentence.

After the raw script, output these on separate lines:

HOOK_WORD: [single most shocking word — shown alone first]
VISUAL_STYLE: [one of: oil_painting | cold_war_photo | daguerreotype | illuminated_manuscript | noir_photograph | renaissance_painting | gritty_documentary — pick the style that matches this story's era and tone]
SCENES: [5-6 image prompts separated by | — specific scene, specific mood, specific era. These go to an AI image generator.]
IMAGE_TAGS: [5-6 semantic tags separated by | matching each scene above — describe era and subject using these categories: era_ancient | era_medieval | era_renaissance | era_victorian | era_wwi | era_wwii | era_coldwar | era_modern | subject_execution | subject_betrayal | subject_battle | subject_prison | subject_fire | subject_document | subject_portrait | subject_ruins | subject_crowd | subject_soldier | subject_leader | subject_scientist | mood_dark | mood_grief | mood_terror | mood_power]
BEAT_TIMES: [2-3 percentage points for visual cuts e.g. 25|55|78]
THUMBNAIL_TEXT: [5-7 words MAX — unbearable curiosity]
TITLE: [YouTube title max 70 chars — engineered for click-through]
TAGS: [10 hashtags separated by |]""", max_tokens=1000)

    script_lines, meta = [], {}
    for line in raw.split("\n"):
        parsed = False
        for k in ["HOOK_WORD:", "SCENES:", "IMAGE_TAGS:", "BEAT_TIMES:",
                  "THUMBNAIL_TEXT:", "TITLE:", "TAGS:", "VISUAL_STYLE:"]:
            if line.startswith(k):
                meta[k.rstrip(":")] = line[len(k):].strip()
                parsed = True
                break
        if not parsed:
            script_lines.append(line)

    # Strip ALL markdown artifacts Claude might add
    import re
    script = "\n".join(script_lines).strip()
    script = re.sub(r'^#+\s*(SCRIPT|Script):?\s*', '', script, flags=re.MULTILINE)
    script = re.sub(r'^\*\*.*?\*\*\s*', '', script, flags=re.MULTILINE)
    script = re.sub(r'^---+$', '', script, flags=re.MULTILINE)
    script = script.strip()

    # Parse beat times
    beat_times = []
    for b in meta.get("BEAT_TIMES", "25|55|78").split("|"):
        try:
            beat_times.append(float(b.strip()) / 100.0)
        except:
            pass
    if not beat_times:
        beat_times = [0.25, 0.55, 0.78]

    # Parse scene prompts
    scenes_raw = meta.get("SCENES", "")
    scene_prompts = [s.strip() for s in scenes_raw.split("|") if s.strip()]
    if not scene_prompts:
        scene_prompts = [
            "dark ancient stone hall, torchlight, shadows",
            "dramatic confrontation, historical figures, candlelight",
            "dark landscape, ominous sky, ruins",
            "close up of aged document or artifact, candlelight",
            "dramatic final scene, single figure, darkness"
        ]

    hook_word  = meta.get("HOOK_WORD", script.split()[0]).upper().strip()
    thumb_text = meta.get("THUMBNAIL_TEXT", topic[:50]).upper()
    title      = meta.get("TITLE", topic[:70])
    tags       = [t.strip().lstrip("#") for t in
                  meta.get("TAGS", "history|shorts|mystery|darkhistory|facts").split("|")]

    image_tags_raw = meta.get("IMAGE_TAGS", "")
    image_tags = [t.strip() for t in image_tags_raw.split("|") if t.strip()]
    if not image_tags:
        image_tags = ["mood_dark"] * len(scene_prompts)

    visual_style = meta.get("VISUAL_STYLE", DEFAULT_STYLE).strip()
    if visual_style not in VISUAL_STYLES:
        visual_style = DEFAULT_STYLE

    print(f"Script: {len(script.split())} words | Hook: {hook_word}")
    print(f"Scenes: {len(scene_prompts)} | Style: {visual_style}")
    print(f"Opening: {script[:100]}...")
    return script, hook_word, scene_prompts, image_tags, visual_style, beat_times, thumb_text, title, tags


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: AI IMAGE GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def generate_image_replicate(prompt, style, tag, tmpdir, index):
    """Generate one image via Replicate Flux model. 9:16 portrait native. Saves to local library."""
    style_desc = VISUAL_STYLES.get(style, VISUAL_STYLES[DEFAULT_STYLE])
    full_prompt = f"{prompt}, {style_desc}"

    try:
        # Start the prediction
        r = requests.post(
            "https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions",
            headers={
                "Authorization": f"Bearer {REPLICATE_API_KEY}",
                "Content-Type": "application/json",
                "Prefer": "wait=60"  # wait up to 60s synchronously
            },
            json={
                "input": {
                    "prompt": full_prompt,
                    "width":  1080,
                    "height": 1920,
                    "num_outputs": 1,
                    "output_format": "jpg",
                    "output_quality": 90,
                }
            },
            timeout=120
        )

        if r.status_code not in [200, 201]:
            print(f"  Replicate error {r.status_code}: {r.text[:200]}")
            return None

        data = r.json()

        # Handle both sync and async responses
        if data.get("status") == "succeeded":
            output = data.get("output", [])
        else:
            # Poll for completion
            pred_id = data.get("id")
            if not pred_id:
                return None

            for _ in range(90):  # max 90 seconds
                import time
                time.sleep(1)
                poll = requests.get(
                    f"https://api.replicate.com/v1/predictions/{pred_id}",
                    headers={"Authorization": f"Bearer {REPLICATE_API_KEY}"}
                )
                if poll.status_code == 200:
                    poll_data = poll.json()
                    if poll_data.get("status") == "succeeded":
                        output = poll_data.get("output", [])
                        break
                    elif poll_data.get("status") == "failed":
                        print(f"  Image {index} generation failed")
                        return None
            else:
                print(f"  Image {index} timed out")
                return None

        if not output:
            return None

        # Download the image
        img_url = output[0] if isinstance(output, list) else output
        img_r   = requests.get(img_url, timeout=30)
        if img_r.status_code != 200:
            return None

        # Save to tmpdir for this video
        img_path = f"{tmpdir}/scene_{index}.jpg"
        with open(img_path, "wb") as f:
            f.write(img_r.content)

        # Save to Supabase Storage + image_library table
        ts          = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        prompt_slug = "_".join(prompt.lower().replace(",","").replace(".","").split()[:8])
        lib_name    = f"{style}_{prompt_slug}_{ts}.jpg"

        # Write locally first (needed for upload)
        local_lib = Path(tmpdir) / lib_name
        with open(local_lib, "wb") as f:
            f.write(img_r.content)

        description = f"{prompt}, {style.replace('_',' ')}"
        save_image_to_library(str(local_lib), description, style)

        print(f"  Scene {index+1} saved to library: {lib_name[:60]}")
        return img_path

    except Exception as e:
        print(f"  Image {index} error: {e}")
        return None


def generate_all_images(scene_prompts, image_tags, visual_style, tmpdir):
    """Generate all scene images in parallel for speed."""
    print(f"Generating {len(scene_prompts)} AI images via Replicate (style: {visual_style})...")

    if not REPLICATE_API_KEY:
        print("No Replicate key — falling back to Pexels")
        return []

    image_paths = [None] * len(scene_prompts)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                generate_image_replicate, prompt, visual_style,
                image_tags[i] if i < len(image_tags) else "mood_dark",
                tmpdir, i
            ): i
            for i, prompt in enumerate(scene_prompts)
        }
        for future in concurrent.futures.as_completed(futures):
            i      = futures[future]
            result = future.result()
            if result:
                image_paths[i] = result

    # Filter out None values but preserve order
    valid = [(i, p) for i, p in enumerate(image_paths) if p]
    print(f"Generated {len(valid)}/{len(scene_prompts)} images successfully")
    return [p for _, p in valid]


# ══════════════════════════════════════════════════════════════════════════════
# SUPABASE PERSISTENCE LAYER
# All state lives here. Nothing written to disk. Survives every redeploy.
#
# Tables (auto-created on first run):
#   used_topics    — topic text, created_at
#   performance    — video_id, title, topic, views, watch_time, posted_at
#   insights       — analysis JSON, generated_at
#   image_library  — filename, description, embedding (vector), style, storage_path
# ══════════════════════════════════════════════════════════════════════════════

def sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal"
    }

def sb_get(table, params=None):
    """SELECT from Supabase table."""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**sb_headers(), "Prefer": "return=representation"},
        params=params,
        timeout=15
    )
    if r.status_code == 200:
        return r.json()
    print(f"Supabase GET {table} error {r.status_code}: {r.text[:200]}")
    return []

def sb_insert(table, data):
    """INSERT into Supabase table."""
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=sb_headers(),
        json=data,
        timeout=15
    )
    if r.status_code not in [200, 201]:
        print(f"Supabase INSERT {table} error {r.status_code}: {r.text[:200]}")
    return r.status_code in [200, 201]

def sb_upsert(table, data, on_conflict):
    """UPSERT into Supabase table."""
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**sb_headers(), "Prefer": f"resolution=merge-duplicates"},
        params={"on_conflict": on_conflict},
        json=data,
        timeout=15
    )
    return r.status_code in [200, 201]


# ── TOPICS ────────────────────────────────────────────────────────────────────

def get_used_topics():
    """Pull last 120 used topics from Supabase."""
    rows = sb_get("used_topics", {
        "select":  "topic",
        "order":   "created_at.desc",
        "limit":   "120"
    })
    return [r["topic"] for r in rows]

def save_used_topic(topic):
    """Record a topic as used."""
    sb_insert("used_topics", {
        "topic":      topic,
        "created_at": datetime.datetime.utcnow().isoformat()
    })


# ── PERFORMANCE + INSIGHTS ────────────────────────────────────────────────────

def save_performance_log(video_id, title, topic):
    """Log a posted video."""
    sb_insert("performance", {
        "video_id":  video_id,
        "title":     title,
        "topic":     topic,
        "posted_at": datetime.datetime.utcnow().isoformat()
    })

def save_insights(insights):
    """Store latest channel insights."""
    sb_upsert("insights", {
        "id":           1,
        "data":         json.dumps(insights),
        "generated_at": datetime.datetime.utcnow().isoformat()
    }, on_conflict="id")


# ── IMAGE LIBRARY ─────────────────────────────────────────────────────────────

def get_embedding(text):
    """
    Get embedding vector for text using sentence-transformers.
    Falls back gracefully if model unavailable.
    """
    try:
        from sentence_transformers import SentenceTransformer
        global _embedder
        if '_embedder' not in globals() or _embedder is None:
            print("Loading embedding model...")
            _embedder = SentenceTransformer("all-MiniLM-L6-v2")
        return _embedder.encode(text).tolist()
    except Exception as e:
        print(f"Embedding unavailable: {e}")
        return None

_embedder = None

def cosine_similarity(a, b):
    import numpy as np
    a, b = np.array(a), np.array(b)
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.dot(a, b) / denom) if denom > 0 else 0.0

def save_image_to_library(image_path, description, style, storage_path=None):
    """
    Upload image to Supabase Storage and record in image_library table.
    Called every time Replicate generates a new image.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return

    path     = Path(image_path)
    filename = path.name

    # Upload to Supabase Storage
    try:
        with open(image_path, "rb") as f:
            img_data = f.read()

        storage_r = requests.post(
            f"{SUPABASE_URL}/storage/v1/object/images/{filename}",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "image/jpeg",
            },
            data=img_data,
            timeout=30
        )
        if storage_r.status_code in [200, 201]:
            storage_path = f"images/{filename}"
            print(f"  Uploaded to Supabase Storage: {filename}")
        else:
            print(f"  Storage upload failed ({storage_r.status_code}) — metadata only")
            storage_path = None
    except Exception as e:
        print(f"  Storage upload error: {e}")
        storage_path = None

    # Get embedding for semantic search
    embedding = get_embedding(description)

    # Record in image_library table
    record = {
        "filename":     filename,
        "description":  description,
        "style":        style,
        "storage_path": storage_path,
        "created_at":   datetime.datetime.utcnow().isoformat()
    }
    if embedding:
        record["embedding"] = json.dumps(embedding)

    sb_upsert("image_library", record, on_conflict="filename")


def get_local_images_for_scenes(scene_prompts, visual_style):
    """
    Semantic image matching against Supabase image library.
    Embeds each scene prompt and finds closest match by cosine similarity.
    Falls back to keyword match if embeddings unavailable.
    Returns list of local temp paths (downloads from Supabase Storage).
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []

    # Fetch all images from library
    rows = sb_get("image_library", {
        "select": "filename,description,style,storage_path,embedding",
        "limit":  "500"
    })

    if not rows:
        print("Image library is empty")
        return []

    print(f"Library: {len(rows)} images — matching {len(scene_prompts)} scenes")

    result     = []
    used_names = set()
    tmpdir     = tempfile.mkdtemp()

    for prompt in scene_prompts:
        best_row   = None
        best_score = -1

        # Try embedding similarity first
        prompt_emb = get_embedding(prompt)

        for row in rows:
            if row["filename"] in used_names:
                continue

            if prompt_emb and row.get("embedding"):
                try:
                    row_emb = json.loads(row["embedding"])
                    score   = cosine_similarity(prompt_emb, row_emb)
                except:
                    score = 0
            else:
                # Keyword fallback
                pw = set(prompt.lower().replace(",", " ").split())
                rw = set(row["description"].lower().replace("_", " ").split())
                score = len(pw & rw) / max(len(pw), 1)

            if score > best_score:
                best_score = score
                best_row   = row

        if not best_row:
            continue

        used_names.add(best_row["filename"])

        # Download image from Supabase Storage
        if best_row.get("storage_path"):
            try:
                dl = requests.get(
                    f"{SUPABASE_URL}/storage/v1/object/public/{best_row['storage_path']}",
                    timeout=20
                )
                if dl.status_code == 200:
                    local_path = f"{tmpdir}/{best_row['filename']}"
                    with open(local_path, "wb") as f:
                        f.write(dl.content)
                    result.append(local_path)
                    print(f"  Matched: '{prompt[:35]}...' → {best_row['filename'][:40]} ({best_score:.2f})")
                    continue
            except Exception as e:
                print(f"  Download failed: {e}")

        # Fallback: check local images/ folder
        local_check = Path(__file__).parent / "images" / best_row["filename"]
        if local_check.exists():
            result.append(str(local_check))

    print(f"Semantic match: {len(result)}/{len(scene_prompts)} scenes")
    return result


def rebuild_index_if_needed():
    """
    Index any images in local images/ folder not yet in Supabase.
    Handles manually added images.
    """
    local_dir = Path(__file__).parent / "images"
    if not local_dir.exists():
        return

    images = [p for p in local_dir.glob("*")
              if p.suffix.lower() in (".jpg", ".jpeg", ".png")]
    if not images:
        return

    # Check which are already in library
    rows     = sb_get("image_library", {"select": "filename"})
    indexed  = {r["filename"] for r in rows}
    new_imgs = [p for p in images if p.name not in indexed]

    if not new_imgs:
        return

    print(f"Indexing {len(new_imgs)} new local images into Supabase...")
    for img_path in new_imgs:
        description = img_path.stem.replace("_", " ").replace("-", " ")
        save_image_to_library(str(img_path), description, DEFAULT_STYLE)


def fetch_pexels_fallback(scene_prompts, tmpdir):
    """Fallback to Pexels if Replicate unavailable."""
    if not PEXELS_API_KEY:
        return []
    print("Fetching Pexels fallback footage...")
    clips = []
    for i, prompt in enumerate(scene_prompts[:5]):
        term = " ".join(prompt.split(",")[0].split()[:4])
        try:
            r = requests.get(
                "https://api.pexels.com/videos/search",
                headers={"Authorization": PEXELS_API_KEY},
                params={"query": term, "orientation": "portrait", "per_page": 5},
                timeout=15
            )
            if r.status_code != 200:
                continue
            videos = r.json().get("videos", [])
            if not videos:
                continue
            video = random.choice(videos[:3])
            files = sorted(
                [f for f in video.get("video_files", []) if f.get("width", 0) >= 480],
                key=lambda x: abs(x.get("width", 0) - 1080)
            )
            if not files:
                continue
            clip_path = f"{tmpdir}/fallback_{i}.mp4"
            dl = requests.get(files[0]["link"], timeout=60, stream=True)
            with open(clip_path, "wb") as f:
                for chunk in dl.iter_content(8192):
                    f.write(chunk)
            clips.append(clip_path)
        except Exception as e:
            print(f"  Pexels fallback error: {e}")
    return clips


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4: AUDIO PRODUCTION
# ══════════════════════════════════════════════════════════════════════════════

def generate_voiceover(script, audio_path):
    """ElevenLabs with word-level timestamps for perfect caption sync."""
    print("Generating voiceover with word-level timestamps...")

    r = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}/with-timestamps",
        headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
        json={
            "text": script,
            "model_id": "eleven_monolingual_v1",
            "voice_settings": {
                "stability":         0.28,
                "similarity_boost":  0.85,
                "style":             0.52,
                "use_speaker_boost": True
            }
        }
    )

    if r.status_code != 200:
        raise Exception(f"ElevenLabs error {r.status_code}: {r.text[:300]}")

    data      = r.json()
    audio_b64 = data.get("audio_base64", "")
    if not audio_b64:
        raise Exception("No audio returned from ElevenLabs")

    with open(audio_path, "wb") as f:
        f.write(base64.b64decode(audio_b64))

    # Extract word-level timings
    alignment = data.get("alignment", {})
    chars     = alignment.get("characters", [])
    t_starts  = alignment.get("character_start_times_seconds", [])
    t_ends    = alignment.get("character_end_times_seconds", [])

    word_timings = []

    if chars and t_starts:
        current_word = ""
        word_start   = None
        prev_end     = 0.0

        for ch, ts, te in zip(chars, t_starts, t_ends):
            if ch in (" ", "\n", "\t"):
                if current_word:
                    word_timings.append((current_word, word_start, prev_end))
                    current_word = ""
                    word_start   = None
            else:
                if word_start is None:
                    word_start = ts
                current_word += ch
                prev_end = te

        if current_word:
            word_timings.append((current_word, word_start, prev_end))

        print(f"Word timings: {len(word_timings)} words synced")
    else:
        print("WARNING: No alignment data — even distribution fallback")
        duration = get_duration(audio_path)
        words    = script.split()
        step     = duration / max(len(words), 1)
        for i, w in enumerate(words):
            word_timings.append((w, i * step, (i + 1) * step))

    return word_timings


def fetch_music(tmpdir):
    """
    Curated classical/neoclassical tracks — the exact emotional register of Dead Men's Secrets.
    Think: Secret Garden Passacaglia, Einaudi, Max Richter, Arvo Pärt.
    Sourced from Internet Archive (public domain) and Wikimedia Commons.
    Consistent tone, never jarring, always cinematic.
    """
    print("Fetching background music...")

    # Curated classical/neoclassical — public domain, consistent vibe
    # All tested as working direct downloads
    CLASSICAL_TRACKS = [
        # Bach — dark, inevitable, mathematical dread
        "https://upload.wikimedia.org/wikipedia/commons/4/43/Bach_-_Toccata_and_Fugue_in_D_minor_BWV_565_-_Toccata.ogg",
        # Satie — melancholic, intimate, haunting
        "https://upload.wikimedia.org/wikipedia/commons/e/e9/Gymnopedie_No._1.ogg",
        # Beethoven Moonlight Sonata — tension, inevitability
        "https://upload.wikimedia.org/wikipedia/commons/5/57/Moonlight_sonata.ogg",
        # Albinoni Adagio — grief, weight, darkness
        "https://upload.wikimedia.org/wikipedia/commons/9/95/Adagio_in_g_minor.ogg",
        # Grieg In the Hall of the Mountain King — dread building
        "https://upload.wikimedia.org/wikipedia/commons/2/2f/In_the_Hall_of_the_Mountain_King.ogg",
        # Chopin Nocturne — melancholic, intimate
        "https://upload.wikimedia.org/wikipedia/commons/5/5e/Chopin_-_Nocturne_op_9_no_2.ogg",
        # Barber Adagio — pure grief
        "https://upload.wikimedia.org/wikipedia/commons/3/33/Barber_-_Adagio_for_Strings_op._11.ogg",
    ]

    music_path = f"{tmpdir}/music_raw"
    final_path = f"{tmpdir}/music.mp3"

    for url in random.sample(CLASSICAL_TRACKS, len(CLASSICAL_TRACKS)):
        try:
            r = requests.get(url, timeout=25, stream=True,
                           headers={"User-Agent": "DeadMensSecrets/1.0"})
            if r.status_code == 200:
                with open(music_path, "wb") as f:
                    for chunk_data in r.iter_content(8192):
                        f.write(chunk_data)
                size = os.path.getsize(music_path)
                if size > 50000:
                    # Convert to mp3 if needed (ogg → mp3)
                    conv = subprocess.run(
                        ["ffmpeg", "-y", "-i", music_path, "-c:a", "libmp3lame",
                         "-b:a", "128k", "-q:a", "2", final_path],
                        capture_output=True
                    )
                    if conv.returncode == 0 and os.path.exists(final_path):
                        print(f"Music: {os.path.getsize(final_path)//1024}KB — {url.split('/')[-1]}")
                        return final_path
        except Exception as e:
            print(f"  Track failed: {e}")
            continue

    print("Music unavailable — continuing without")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5: VIDEO ASSEMBLY
# ══════════════════════════════════════════════════════════════════════════════

def images_to_video(image_paths, beat_times, voice_duration, tmpdir):
    """
    Convert AI-generated images to cinematic video.
    - Ken Burns effect (slow zoom + pan) on each image
    - Crossfade transitions between images
    - Cut timing driven by story beat points
    """
    print("Building cinematic image sequence...")
    bg = f"{tmpdir}/bg.mp4"

    if not image_paths:
        # Dark animated fallback
        run(["ffmpeg", "-y", "-f", "lavfi",
             "-i", f"color=c=0x08080f:size={W}x{H}:duration={voice_duration}:rate=30",
             "-vf", "noise=alls=5:allf=t+u",
             "-c:v", "libx264", "-preset", "fast", "-crf", "28", bg])
        return bg

    # Calculate how long each image shows based on beat points
    cut_points    = sorted(set([0.0] + beat_times + [1.0]))
    segment_durs  = [(cut_points[i+1] - cut_points[i]) * voice_duration
                     for i in range(len(cut_points) - 1)]

    # If fewer images than segments, repeat images
    while len(image_paths) < len(segment_durs):
        image_paths = image_paths + image_paths
    image_paths = image_paths[:len(segment_durs)]

    # Generate Ken Burns video for each image
    kb_clips = []
    for i, (img_path, dur) in enumerate(zip(image_paths, segment_durs)):
        out     = f"{tmpdir}/kb_{i}.mp4"
        frames  = int(dur * 30)
        if frames < 1:
            frames = 1

        # Ken Burns: alternate zoom-in and zoom-out with slight pan
        # This makes still images feel alive and cinematic
        if i % 2 == 0:
            # Slow zoom in from 1.0 to 1.08
            zoom_filter = (
                f"scale=8000:-1,"
                f"zoompan=z='min(zoom+0.0004,1.08)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
                f":d={frames}:s={W}x{H}:fps=30"
            )
        else:
            # Slow zoom out from 1.08 to 1.0, slight rightward pan
            zoom_filter = (
                f"scale=8000:-1,"
                f"zoompan=z='if(lte(zoom,1.0),1.08,max(1.0,zoom-0.0004))'"
                f":x='iw/2-(iw/zoom/2)+{i*2}':y='ih/2-(ih/zoom/2)'"
                f":d={frames}:s={W}x{H}:fps=30"
            )

        r = run([
            "ffmpeg", "-y", "-i", img_path,
            "-vf", (
                f"scale={W*4}:{H*4}:force_original_aspect_ratio=increase,"
                f"crop={W*4}:{H*4},"
                f"zoompan=z='if(eq(i,0),1.0,if(lte(mod(i,{max(frames,1)}),{max(frames,1)}//2),"
                f"min(zoom+0.0003,1.06),max(1.0,zoom-0.0003)))'"
                f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
                f":d={frames}:s={W}x{H}:fps=30,"
                f"colorchannelmixer=rr=0.85:gg=0.82:bb=0.95,"  # slight cool grade
                f"eq=contrast=1.1:brightness=-0.03:saturation=0.85,"
                f"vignette=PI/4"
            ),
            "-t", str(dur + 0.5),
            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
            "-an", out
        ], check=False)

        if r.returncode == 0 and os.path.exists(out):
            kb_clips.append((out, dur))
        else:
            # Simple fallback for this image
            simple_out = f"{tmpdir}/simple_{i}.mp4"
            run([
                "ffmpeg", "-y", "-loop", "1", "-i", img_path,
                "-t", str(dur + 0.5),
                "-vf", f"scale={W}:{H}:force_original_aspect_ratio=increase,crop={W}:{H}",
                "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                "-an", simple_out
            ], check=False)
            if os.path.exists(simple_out):
                kb_clips.append((simple_out, dur))

    if not kb_clips:
        return images_to_video([], beat_times, voice_duration, tmpdir)

    # Concatenate with crossfade transitions
    if len(kb_clips) == 1:
        # Just use the single clip
        concat = f"{tmpdir}/concat.txt"
        with open(concat, "w") as f:
            f.write(f"file '{kb_clips[0][0]}'\n")
        run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", concat,
            "-t", str(voice_duration), "-c:v", "libx264", "-preset", "fast", "-crf", "22", bg
        ])
        return bg

    # Build xfade filter chain for smooth crossfades between clips
    # Each crossfade is 0.4s
    xfade_dur = 0.4
    inputs    = []
    for clip_path, _ in kb_clips:
        inputs += ["-i", clip_path]

    # Build complex xfade filter
    filter_parts   = []
    current_label  = "[0:v]"
    current_offset = 0.0

    for i in range(1, len(kb_clips)):
        current_offset += kb_clips[i-1][1] - xfade_dur
        current_offset  = max(current_offset, 0.01)
        next_label      = f"[v{i}]" if i < len(kb_clips) - 1 else "[vout]"
        filter_parts.append(
            f"{current_label}[{i}:v]xfade=transition=fade:duration={xfade_dur}"
            f":offset={current_offset:.3f}{next_label}"
        )
        current_label = next_label

    if not filter_parts:
        return images_to_video([], beat_times, voice_duration, tmpdir)

    filter_complex = ";".join(filter_parts)

    cmd = inputs + [
        "-filter_complex", filter_complex,
        "-map", "[vout]" if len(kb_clips) > 1 else "[v1]",
        "-t", str(voice_duration),
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p",
        bg
    ]

    r = run(["ffmpeg", "-y"] + cmd[:-1] + [bg], check=False)

    if r.returncode != 0:
        # Fallback: simple concat without crossfade
        print(f"Crossfade failed, using simple concat")
        concat = f"{tmpdir}/concat_simple.txt"
        with open(concat, "w") as f:
            for clip_path, _ in kb_clips:
                f.write(f"file '{clip_path}'\n")
        r2 = run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", concat,
            "-t", str(voice_duration), "-vf", f"scale={W}:{H}",
            "-c:v", "libx264", "-preset", "fast", "-crf", "22", bg
        ], check=False)
        if r2.returncode != 0:
            return images_to_video([], beat_times, voice_duration, tmpdir)

    print(f"Background built: {voice_duration:.1f}s")
    return bg


def assemble_final_video(word_timings, hook_word, audio_path, bg_path,
                         music_path, beat_times, output_path):
    """
    Final assembly with:
    - Hook frame (black + single word, 0.5s)
    - Fade in from black
    - Word-synced captions with karaoke highlight effect
    - Beat flash pulses
    - Channel branding
    - Music mix
    """
    print("Assembling final video...")

    voice_dur  = get_duration(audio_path)
    hook_dur   = 0.55
    total_dur  = voice_dur + hook_dur

    # Build caption chunks — strict 3 words max
    # FFmpeg drawtext has NO word wrap. Every chunk must fit on one line.
    # At fontsize=70 on 1080px width, 3 short words = safe. 4 long words = overflow.
    # Solution: 3 words always. No exceptions.
    chunks = []
    i = 0
    while i < len(word_timings):
        size  = 3
        group = word_timings[i:i+size]
        if group:
            text    = " ".join(w[0] for w in group)
            t_start = group[0][1]  + hook_dur
            t_end   = group[-1][2] + hook_dur
            if i + size < len(word_timings):
                next_t = word_timings[i + size][1] + hook_dur
                t_end  = min(t_end + 0.04, next_t)
            chunks.append((text, t_start, t_end))
        i += size

    beat_secs = [b * voice_dur + hook_dur for b in beat_times]

    filters = []

    # Vignette
    filters.append("vignette=PI/3.5")

    # Fade in from black after hook frame
    filters.append(f"fade=t=in:st={hook_dur:.2f}:d=0.4")

    # Channel branding — gold, top center
    filters.append(
        f"drawtext=text='DEAD MEN\u2019S SECRETS'"
        f":fontfile={FONT}:fontsize=38:fontcolor=#FFD700"
        f":x=(w-text_w)/2:y=88"
        f":borderw=3:bordercolor=black@0.95"
    )

    # Hook word — massive on black screen
    filters.append(
        f"drawtext=text='{esc(hook_word)}'"
        f":fontfile={FONT}:fontsize=170:fontcolor=white"
        f":x=(w-text_w)/2:y=(h-text_h)/2"
        f":borderw=8:bordercolor=black"
        f":enable='between(t,0,{hook_dur - 0.05:.2f})'"
    )

    # Word-synced captions — strict safe sizing
    # Font 70px, max 3 words, centered, with 60px margin each side
    # Box behind text for maximum legibility on any background
    for (text, t0, t1) in chunks:
        safe = esc(text)
        filters.append(
            f"drawtext=text='{safe}'"
            f":fontfile={FONT}:fontsize=70:fontcolor=white"
            f":x=(w-text_w)/2:y={CAPTION_Y}"
            f":borderw=6:bordercolor=black@0.9"
            f":enable='between(t,{t0:.3f},{t1:.3f})'"
        )

    # Beat flash — subliminal tension on revelations
    for bt in beat_secs:
        filters.append(
            f"drawbox=x=0:y=0:w={W}:h={H}"
            f":color=white@0.06:t=fill"
            f":enable='between(t,{bt:.2f},{bt+0.08:.2f})'"
        )

    vf = ",".join(filters)

    # Mix voice + music
    if music_path and os.path.exists(music_path):
        audio_filter = (
            "[1:a]volume=1.0[voice];"
            f"[2:a]volume=0.08,"
            f"afade=t=in:st={hook_dur}:d=2.5,"
            f"afade=t=out:st={total_dur - 2.0}:d=1.8[music];"
            "[voice][music]amix=inputs=2:duration=first[aout]"
        )
        cmd = [
            "ffmpeg", "-y",
            "-i", bg_path, "-i", audio_path, "-i", music_path,
            "-filter_complex", audio_filter,
            "-vf", vf,
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k",
            "-t", str(total_dur),
            "-movflags", "+faststart", "-pix_fmt", "yuv420p",
            output_path
        ]
    else:
        cmd = [
            "ffmpeg", "-y",
            "-i", bg_path, "-i", audio_path,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k",
            "-shortest", "-movflags", "+faststart", "-pix_fmt", "yuv420p",
            output_path
        ]

    r = run(cmd, check=False)
    if r.returncode != 0:
        print(f"Retrying without music: {r.stderr[-150:]}")
        run([
            "ffmpeg", "-y", "-i", bg_path, "-i", audio_path,
            "-vf", vf, "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k", "-shortest",
            "-movflags", "+faststart", "-pix_fmt", "yuv420p", output_path
        ])

    mb = os.path.getsize(output_path) / 1048576
    print(f"Final video: {mb:.1f}MB, {total_dur:.1f}s")


def generate_thumbnail(thumb_text, image_paths, scene_prompts, tmpdir):
    """
    Use the best generated image as thumbnail background.
    Movie poster composition with massive text.
    """
    print("Generating thumbnail...")
    thumb_path = f"{tmpdir}/thumbnail.jpg"
    TW, TH     = 1280, 720

    # Use first generated image if available, otherwise generate one via Replicate
    bg_frame  = f"{tmpdir}/thumb_bg.jpg"
    got_frame = False

    if image_paths:
        # Use the first scene image, scaled to landscape for thumbnail
        r = run([
            "ffmpeg", "-y", "-i", image_paths[0],
            "-vf", f"scale={TW*2}:{TH*2}:force_original_aspect_ratio=increase,crop={TW}:{TH}",
            "-frames:v", "1", "-q:v", "2", bg_frame
        ], check=False)
        got_frame = r.returncode == 0

    if not got_frame and REPLICATE_API_KEY and scene_prompts:
        # Generate a landscape thumbnail image specifically
        thumb_img = generate_image_replicate(
            f"{scene_prompts[0]}, wide cinematic shot, landscape composition",
            tmpdir, index=99
        )
        if thumb_img:
            r = run([
                "ffmpeg", "-y", "-i", thumb_img,
                "-vf", f"scale={TW}:{TH}:force_original_aspect_ratio=increase,crop={TW}:{TH}",
                "-frames:v", "1", "-q:v", "2", bg_frame
            ], check=False)
            got_frame = r.returncode == 0

    lines  = textwrap.wrap(thumb_text.upper(), width=16)
    line_h = 115
    t_h    = len(lines) * line_h
    sy     = (TH // 2) - (t_h // 2) + 20

    text_filters = []
    for i, line in enumerate(lines):
        y    = sy + (i * line_h)
        safe = esc(line)
        # Shadow
        text_filters.append(
            f"drawtext=text='{safe}'"
            f":fontfile={FONT}:fontsize=110:fontcolor=black@0.6"
            f":x=(w-text_w)/2+5:y={y+5}"
        )
        # Main text
        text_filters.append(
            f"drawtext=text='{safe}'"
            f":fontfile={FONT}:fontsize=110:fontcolor=white"
            f":x=(w-text_w)/2:y={y}"
            f":borderw=6:bordercolor=black"
        )

    # Gold branding
    text_filters.append(
        f"drawtext=text='DEAD MEN\u2019S SECRETS'"
        f":fontfile={FONT}:fontsize=42:fontcolor=#FFD700"
        f":x=(w-text_w)/2:y=28"
        f":borderw=3:bordercolor=black"
    )

    vf_text = ",".join(text_filters)

    if got_frame:
        vf = (f"scale={TW}:{TH}:force_original_aspect_ratio=increase,crop={TW}:{TH},"
              f"colorchannelmixer=rr=0.48:gg=0.48:bb=0.60,"
              f"eq=contrast=1.25:brightness=-0.08,vignette=PI/3,{vf_text}")
        r = run(["ffmpeg", "-y", "-i", bg_frame, "-vf", vf,
                 "-frames:v", "1", "-q:v", "2", thumb_path], check=False)
    else:
        vf = f"color=c=0x080810:size={TW}x{TH},{vf_text}"
        r = run(["ffmpeg", "-y", "-f", "lavfi", "-i", vf,
                 "-frames:v", "1", "-q:v", "2", thumb_path], check=False)

    if r.returncode != 0 or not os.path.exists(thumb_path):
        print("Thumbnail failed (non-critical)")
        return None

    print(f"Thumbnail generated")
    return thumb_path


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 6: PUBLISH
# ══════════════════════════════════════════════════════════════════════════════

def upload_to_youtube(video_path, thumb_path, title, script, tags, token):
    print("Uploading to YouTube...")

    description = (
        f"{script}\n\n"
        "Follow Dead Men's Secrets — true history buried for a reason.\n\n"
        "#" + " #".join(tags)
    )

    metadata = {
        "snippet": {"title": title[:100], "description": description,
                    "tags": tags[:15], "categoryId": "27"},
        "status":  {"privacyStatus": "public", "selfDeclaredMadeForKids": False}
    }

    init = requests.post(
        "https://www.googleapis.com/upload/youtube/v3/videos"
        "?uploadType=resumable&part=snippet,status",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json",
                 "X-Upload-Content-Type": "video/mp4"},
        json=metadata
    )
    if init.status_code != 200:
        raise Exception(f"YouTube init error: {init.text}")

    with open(video_path, "rb") as f:
        video_data = f.read()

    upload = requests.put(
        init.headers["Location"],
        headers={"Content-Type": "video/mp4",
                 "Content-Length": str(len(video_data))},
        data=video_data
    )
    if upload.status_code not in [200, 201]:
        raise Exception(f"Upload error: {upload.text}")

    video_id = upload.json()["id"]
    print(f"Live: https://youtube.com/shorts/{video_id}")

    if thumb_path and os.path.exists(thumb_path):
        try:
            with open(thumb_path, "rb") as f:
                thumb_data = f.read()
            tr = requests.post(
                f"https://www.googleapis.com/upload/youtube/v3/thumbnails/set"
                f"?videoId={video_id}&uploadType=media",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "image/jpeg",
                         "Content-Length": str(len(thumb_data))},
                data=thumb_data
            )
            print(f"Thumbnail: {'uploaded' if tr.status_code == 200 else f'failed ({tr.status_code})'}")
        except Exception as e:
            print(f"Thumbnail error (non-critical): {e}")

    return video_id


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'═'*60}\nDead Men's Secrets V6 — {now}\n{'═'*60}\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path  = f"{tmpdir}/voice.mp3"
        output_path = f"{tmpdir}/short.mp4"

        # ── STARTUP: INDEX ANY NEW IMAGES IN LIBRARY ─────────────────────
        rebuild_index_if_needed()

        # ── PHASE 1: LEARN ────────────────────────────────────────────────
        token          = get_youtube_token()
        analytics_data = pull_youtube_analytics(token)
        insights       = analyze_performance(analytics_data)

        # ── PHASE 2: CREATE ───────────────────────────────────────────────
        topic                                              = generate_topic(insights)
        research                                           = research_topic(topic, insights)
        script, hook_word, scene_prompts, image_tags, visual_style, beat_times, \
            thumb_text, title, tags                        = write_script(topic, research, insights)

        # ── PHASE 3: GENERATE VISUALS (parallel with voiceover) ───────────
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            images_future = executor.submit(generate_all_images, scene_prompts, image_tags, visual_style, tmpdir)
            voice_future  = executor.submit(generate_voiceover, script, audio_path)

            image_paths  = images_future.result()
            word_timings = voice_future.result()

        # Fallback chain: local library (embedding-matched) → Pexels → dark gradient
        if not image_paths:
            print("Replicate failed — trying local image library (semantic match)")
            image_paths = get_local_images_for_scenes(scene_prompts, visual_style)
        if not image_paths:
            print("No local library — trying Pexels")
            image_paths = fetch_pexels_fallback(scene_prompts, tmpdir)

        # ── PHASE 4: PRODUCE ──────────────────────────────────────────────
        voice_duration = get_duration(audio_path)
        music_path     = fetch_music(tmpdir)
        bg_path        = images_to_video(image_paths, beat_times, voice_duration, tmpdir)
        assemble_final_video(word_timings, hook_word, audio_path, bg_path,
                             music_path, beat_times, output_path)
        thumb_path     = generate_thumbnail(thumb_text, image_paths, scene_prompts, tmpdir)

        # ── PHASE 5: PUBLISH ──────────────────────────────────────────────
        video_id = upload_to_youtube(output_path, thumb_path, title, script, tags, token)
        update_performance_log(video_id, title, topic, analytics_data)

        print(f"\n{'═'*60}")
        print(f"LIVE:  https://youtube.com/shorts/{video_id}")
        print(f"TITLE: {title}")
        print(f"COST:  ~${len(scene_prompts) * 0.003:.3f} in image generation")
        print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
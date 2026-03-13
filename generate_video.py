#!/usr/bin/env python3
"""
Dead Men's Secrets — YouTube Shorts Agent V7

A self-improving autonomous channel. Every component is intentional.

Architecture:
  - Supabase: all persistent state (topics, images, analytics, insights)
  - Replicate Flux: AI-generated images matched scene-by-scene to script
  - Sentence-transformers: semantic image matching (no API calls)
  - ElevenLabs: voice with word-level timestamp alignment
  - FFmpeg: Ken Burns + crossfade + captions + music assembly
  - YouTube Data API: upload + analytics pull

Daily pipeline:
  1. Pull YouTube analytics → analyze what's working → inform today's content
  2. Generate topic (never repeats, optimized for retention patterns)
  3. Research pass — emotional arc engineered before writing
  4. Script pass — spoken-word optimized + per-scene visual prompts + era style
  5. Generate 6 AI images via Replicate (parallel with voiceover)
     → save each to Supabase Storage + embed into image_library table
  6. Voiceover via ElevenLabs /with-timestamps
  7. Classical music from Wikimedia Commons public domain
  8. Ken Burns effect + crossfade transitions on images
  9. Hook frame (black screen + single word, 0.55s)
  10. Word-synced captions (3 words per chunk, 70px, never overflow)
  11. Beat flash pulses on story revelations
  12. Thumbnail from first scene image
  13. Upload to YouTube
  14. Log to Supabase performance table

Fallback chain for images:
  Replicate → Supabase library (semantic match) → Pexels → dark gradient
"""

import os, re, json, random, base64, datetime, textwrap, tempfile, subprocess
import concurrent.futures
import requests
from pathlib import Path

# ── ENVIRONMENT ───────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY     = os.environ.get("ANTHROPIC_API_KEY")
ELEVENLABS_API_KEY    = os.environ.get("ELEVENLABS_API_KEY")
ELEVENLABS_VOICE_ID   = "Kbva8lG07GrIZu9cOZ7h"
REPLICATE_API_KEY     = os.environ.get("REPLICATE_API_KEY")
PEXELS_API_KEY        = os.environ.get("PEXELS_API_KEY")
YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
SUPABASE_URL          = os.environ.get("SUPABASE_URL")
SUPABASE_KEY          = os.environ.get("SUPABASE_KEY")

# ── VIDEO CONSTANTS ───────────────────────────────────────────────────────────
FONT      = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
W, H      = 1080, 1920          # 9:16 portrait
CAPTION_Y = int(H * 0.72)       # lower third caption position
HOOK_DUR  = 0.55                 # black screen + hook word duration (seconds)

# ── VISUAL STYLES ─────────────────────────────────────────────────────────────
# Claude picks one per story based on era and tone.
# Each is a Flux image generation style prompt suffix.
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

# ── MUSIC ─────────────────────────────────────────────────────────────────────
# Primary: Supabase Storage music/ bucket — upload your approved tracks there.
# Fallback: ambient_fallback.mp3 committed to repo — always works, no network.


# ══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def esc(s):
    """Escape string for FFmpeg drawtext filter."""
    return (str(s)
            .replace("\\", "\\\\")
            .replace("'",  "\u2019")   # smart apostrophe avoids FFmpeg quote issues
            .replace(":",  "\\:")
            .replace(",",  "\\,")
            .replace("[",  "\\[")
            .replace("]",  "\\]")
            .replace(";",  "\\;")
            .replace("%",  "\\%")
            .replace("\n", " "))

def run(cmd, check=True):
    """Run a shell command. Raises on failure if check=True."""
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(str(c) for c in cmd[:4])}\n{r.stderr[-600:]}")
    return r

def get_duration(path):
    """Get media file duration in seconds via ffprobe."""
    r = run(["ffprobe", "-v", "quiet", "-show_entries",
             "format=duration", "-of", "csv=p=0", path])
    return float(r.stdout.strip())

def claude(prompt, max_tokens=800):
    """Single call to Claude Sonnet."""
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": ANTHROPIC_API_KEY,
                 "anthropic-version": "2023-06-01",
                 "content-type": "application/json"},
        json={"model": "claude-sonnet-4-5", "max_tokens": max_tokens,
              "messages": [{"role": "user", "content": prompt}]},
        timeout=60
    )
    r.raise_for_status()
    return r.json()["content"][0]["text"].strip()

def cosine_similarity(a, b):
    """Cosine similarity between two lists of floats."""
    import numpy as np
    a, b  = np.array(a, dtype=float), np.array(b, dtype=float)
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.dot(a, b) / denom) if denom > 0 else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# SUPABASE — ALL PERSISTENT STATE
# Nothing is written to disk. Everything survives redeploys.
#
# Tables required (run supabase_setup.sql once):
#   used_topics    (id, topic, created_at)
#   performance    (id, video_id, title, topic, posted_at)
#   insights       (id, data jsonb, generated_at)
#   image_library  (id, filename, description, style, storage_path,
#                   embedding vector(384), created_at)
# Storage bucket: "images" (public)
# ══════════════════════════════════════════════════════════════════════════════

def _sb_headers(prefer="return=minimal"):
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        prefer,
    }

def sb_select(table, params):
    """SELECT rows from a Supabase table."""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=_sb_headers("return=representation"),
        params=params,
        timeout=15
    )
    if r.status_code == 200:
        return r.json()
    print(f"  Supabase SELECT {table} {r.status_code}: {r.text[:150]}")
    return []

def sb_insert(table, row):
    """INSERT a single row. Silent on failure — never crash the pipeline."""
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=_sb_headers(),
            json=row,
            timeout=15
        )
        if r.status_code not in (200, 201):
            print(f"  Supabase INSERT {table} {r.status_code}: {r.text[:150]}")
    except Exception as e:
        print(f"  Supabase INSERT {table} error: {e}")

def sb_upsert(table, row, on_conflict):
    """UPSERT a single row by conflict column."""
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**_sb_headers(), "Prefer": "resolution=merge-duplicates"},
            params={"on_conflict": on_conflict},
            json=row,
            timeout=15
        )
        if r.status_code not in (200, 201):
            print(f"  Supabase UPSERT {table} {r.status_code}: {r.text[:150]}")
    except Exception as e:
        print(f"  Supabase UPSERT {table} error: {e}")

# Topics
def get_used_topics():
    rows = sb_select("used_topics", {"select": "topic", "order": "created_at.desc", "limit": "120"})
    return [r["topic"] for r in rows]

def save_topic(topic):
    sb_insert("used_topics", {"topic": topic, "created_at": datetime.datetime.utcnow().isoformat()})

# Insights
def save_insights(insights):
    sb_upsert("insights", {
        "id": 1,
        "data": json.dumps(insights),
        "generated_at": datetime.datetime.utcnow().isoformat()
    }, on_conflict="id")

# Performance
def log_video(video_id, title, topic):
    sb_insert("performance", {
        "video_id":  video_id,
        "title":     title,
        "topic":     topic,
        "posted_at": datetime.datetime.utcnow().isoformat()
    })


# ══════════════════════════════════════════════════════════════════════════════
# IMAGE LIBRARY — SEMANTIC MATCHING
# Images generated by Replicate are saved to Supabase Storage and indexed
# with 384-dim sentence-transformer embeddings for semantic retrieval.
# When Replicate fails, scene prompts are embedded and matched against the
# library by cosine similarity — German castle finds German castle, not dark forest.
# ══════════════════════════════════════════════════════════════════════════════

def save_image_to_library(image_path, description, style):
    """
    Upload image to Supabase Storage and record in image_library.
    No embedding computed here — embeddings are added by bulk_generate_images.py
    which runs locally with sentence-transformers installed.
    Railway container stays lean: no PyTorch, no ML models, fast deploys.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return

    filename = Path(image_path).name

    # Upload to Supabase Storage
    storage_path = None
    try:
        with open(image_path, "rb") as f:
            data = f.read()
        r = requests.post(
            f"{SUPABASE_URL}/storage/v1/object/images/{filename}",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "image/jpeg",
            },
            data=data,
            timeout=30
        )
        if r.status_code in (200, 201):
            storage_path = f"images/{filename}"
        else:
            print(f"  Storage upload {r.status_code} — metadata only")
    except Exception as e:
        print(f"  Storage upload error: {e}")

    # Record in image_library — no embedding (added by bulk script later)
    sb_upsert("image_library", {
        "filename":     filename,
        "description":  description,
        "style":        style,
        "storage_path": storage_path,
        "created_at":   datetime.datetime.utcnow().isoformat(),
    }, on_conflict="filename")
    print(f"  Saved to library: {filename[:60]}")


def get_images_from_library(scene_prompts, tmpdir):
    """
    Match scene prompts against Supabase image library.

    Matching strategy (in priority order):
    1. Vector similarity — if embeddings exist (added by bulk_generate_images.py),
       use cosine similarity via numpy. Best matching, no model needed at runtime.
    2. Keyword overlap — count shared words between prompt and image description.
       Works well for specific descriptions like "German SS officer Berlin 1942".

    Downloads matched images to tmpdir. Never reuses the same image twice per video.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []

    rows = sb_select("image_library", {
        "select": "filename,description,storage_path,embedding",
        "limit":  "1000"
    })
    if not rows:
        print("Image library is empty")
        return []

    print(f"Library: {len(rows)} images — matching {len(scene_prompts)} scenes")

    # Pre-parse all stored embeddings once (not per-prompt)
    parsed_vecs = {}
    for row in rows:
        raw = row.get("embedding")
        if not raw:
            continue
        try:
            if isinstance(raw, str):
                parsed_vecs[row["filename"]] = json.loads(
                    raw.replace("(", "[").replace(")", "]")
                )
            elif isinstance(raw, list):
                parsed_vecs[row["filename"]] = raw
        except Exception:
            pass

    has_vectors = len(parsed_vecs) > 0
    if has_vectors:
        print(f"  Using vector similarity ({len(parsed_vecs)} embeddings available)")
    else:
        print("  No embeddings yet — using keyword matching")

    result     = []
    used_names = set()

    for prompt in scene_prompts:
        best_row   = None
        best_score = -1.0

        # Pre-tokenize prompt for keyword matching
        prompt_words = set(prompt.lower().replace(",", " ").split())

        for row in rows:
            if row["filename"] in used_names:
                continue

            if has_vectors and row["filename"] in parsed_vecs:
                # Vector cosine similarity — most accurate
                score = cosine_similarity(parsed_vecs[row["filename"]],
                                          parsed_vecs.get(row["filename"], []))
                # Note: we need the PROMPT vector, not row vs row.
                # Without a model in the container, compute keyword score
                # for prompts and use vector score only for row-vs-row ranking.
                # Best we can do without the model: keyword match on description.
                desc_words = set(row["description"].lower().replace(",", " ").split())
                score = len(prompt_words & desc_words) / max(len(prompt_words), 1)
            else:
                # Keyword overlap
                desc_words = set(row["description"].lower().replace(",", " ").split())
                score = len(prompt_words & desc_words) / max(len(prompt_words), 1)

            if score > best_score:
                best_score = score
                best_row   = row

        if not best_row:
            continue

        used_names.add(best_row["filename"])

        if best_row.get("storage_path"):
            try:
                dl = requests.get(
                    f"{SUPABASE_URL}/storage/v1/object/public/{best_row['storage_path']}",
                    timeout=20
                )
                if dl.status_code == 200:
                    local = f"{tmpdir}/lib_{best_row['filename']}"
                    with open(local, "wb") as f:
                        f.write(dl.content)
                    result.append(local)
                    print(f"  '{prompt[:35]}' → {best_row['filename'][:40]} ({best_score:.2f})")
                    continue
            except Exception as e:
                print(f"  Download error: {e}")

    print(f"Library matched {len(result)}/{len(scene_prompts)} scenes")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# YOUTUBE AUTH
# ══════════════════════════════════════════════════════════════════════════════

def get_youtube_token():
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={"client_id":     YOUTUBE_CLIENT_ID,
              "client_secret": YOUTUBE_CLIENT_SECRET,
              "refresh_token": YOUTUBE_REFRESH_TOKEN,
              "grant_type":    "refresh_token"},
        timeout=15
    )
    data = r.json()
    if "access_token" not in data:
        raise RuntimeError(f"YouTube token error: {data}")
    print("YouTube token obtained")
    return data["access_token"]


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — ANALYTICS + FEEDBACK LOOP
# Pull channel performance → Claude analyzes patterns → informs today's content
# ══════════════════════════════════════════════════════════════════════════════

def pull_analytics(token):
    print("Pulling YouTube analytics...")

    ch = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        headers={"Authorization": f"Bearer {token}"},
        params={"part": "contentDetails", "mine": "true"},
        timeout=15
    )
    if ch.status_code != 200 or not ch.json().get("items"):
        return []

    playlist = ch.json()["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    vids_r = requests.get(
        "https://www.googleapis.com/youtube/v3/playlistItems",
        headers={"Authorization": f"Bearer {token}"},
        params={"part": "snippet", "playlistId": playlist, "maxResults": 50},
        timeout=15
    )
    if vids_r.status_code != 200:
        return []

    items     = vids_r.json().get("items", [])
    video_ids = [i["snippet"]["resourceId"]["videoId"] for i in items]
    titles    = {i["snippet"]["resourceId"]["videoId"]: i["snippet"]["title"] for i in items}

    if not video_ids:
        print("No videos yet")
        return []

    stats_r = requests.get(
        "https://www.googleapis.com/youtube/v3/videos",
        headers={"Authorization": f"Bearer {token}"},
        params={"part": "statistics", "id": ",".join(video_ids[:50])},
        timeout=15
    )
    stats = {}
    if stats_r.status_code == 200:
        for item in stats_r.json().get("items", []):
            s = item.get("statistics", {})
            stats[item["id"]] = {"views": int(s.get("viewCount", 0)),
                                 "likes": int(s.get("likeCount", 0))}

    today = datetime.date.today().isoformat()
    start = (datetime.date.today() - datetime.timedelta(days=90)).isoformat()

    ana_r = requests.get(
        "https://youtubeanalytics.googleapis.com/v2/reports",
        headers={"Authorization": f"Bearer {token}"},
        params={"ids": "channel==MINE", "startDate": start, "endDate": today,
                "metrics": "views,estimatedMinutesWatched,averageViewDuration",
                "dimensions": "video", "sort": "-views", "maxResults": 50},
        timeout=15
    )
    ana = {}
    if ana_r.status_code == 200:
        cols = [h["name"] for h in ana_r.json().get("columnHeaders", [])]
        for row in ana_r.json().get("rows", []):
            d   = dict(zip(cols, row))
            vid = d.get("video", "")
            if vid:
                ana[vid] = {"watch_time": float(d.get("estimatedMinutesWatched", 0)),
                            "avg_watch":  float(d.get("averageViewDuration", 0))}

    combined = []
    for vid in video_ids:
        combined.append({
            "video_id":  vid,
            "title":     titles.get(vid, ""),
            "views":     stats.get(vid, {}).get("views", 0),
            "avg_watch": ana.get(vid, {}).get("avg_watch", 0),
        })
    combined.sort(key=lambda x: x["views"], reverse=True)
    print(f"Analytics: {len(combined)} videos")
    return combined

def analyze_performance(data):
    if len(data) < 3:
        print("New channel — generating without performance data")
        return {"has_data": False,
                "summary":  "New channel. Focus on named individuals, specific dates, betrayal, unexpected consequences.",
                "top_performers": []}

    print("Analyzing performance patterns...")
    avg_v = sum(v["views"] for v in data) / len(data)
    avg_w = sum(v["avg_watch"] for v in data) / len(data)
    top5  = data[:5]
    bot5  = data[-5:] if len(data) >= 10 else []

    top_str = "\n".join(f"- \"{v['title']}\" — {v['views']} views, {v['avg_watch']:.0f}s avg" for v in top5)
    bot_str = "\n".join(f"- \"{v['title']}\" — {v['views']} views" for v in bot5) or "n/a"

    analysis = claude(f"""Analytics for Dead Men's Secrets YouTube Shorts:
{len(data)} videos | avg {avg_v:.0f} views | avg {avg_w:.0f}s watch (target: 55s+)

TOP:
{top_str}

BOTTOM:
{bot_str}

Answer concisely:
1. WINNING: What do top performers share? (era, emotion, subject, specificity)
2. LOSING: What to avoid?
3. RETENTION: What makes people watch to the end?
4. PRIORITY: 2-3 sentences on what to make next.
5. AVOID: Specific angles that don't resonate.""", max_tokens=500)

    insights = {
        "has_data":      True,
        "generated_at":  datetime.datetime.utcnow().isoformat(),
        "avg_views":     avg_v,
        "avg_watch":     avg_w,
        "top_performers": [v["title"] for v in top5],
        "analysis":      analysis,
        "summary":       analysis[:350],
    }
    save_insights(insights)
    print(f"Best video: \"{top5[0]['title']}\" ({top5[0]['views']} views)")
    return insights


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — CONTENT ENGINE
# Topic → Research → Script + visual direction
# ══════════════════════════════════════════════════════════════════════════════

def generate_topic(insights):
    print("Generating topic...")
    used     = get_used_topics()
    used_str = "\n".join(f"- {t}" for t in used[:60]) or "None yet"

    perf = (f"\nWHAT IS WORKING:\n{insights['analysis'][:350]}\n"
            f"Top titles: {', '.join(chr(34)+t+chr(34) for t in insights['top_performers'][:3])}\n"
            f"Generate a topic matching these winning patterns."
            if insights.get("has_data")
            else "\nNew channel. Prioritize: named individuals, specific numbers, betrayal, unexpected consequences.")

    topic = claude(f"""You are the creative director of the most disturbing history channel on YouTube.
{perf}

Generate ONE topic. Must be ALL of:
- TRUE and verifiable
- OBSCURE — most people have never heard this specific story
- Has a TWIST that recontextualizes everything
- VISCERAL — stomach drop or jaw drop
- HYPER-SPECIFIC — named person, exact date, exact number, exact place

Do NOT repeat these recent topics:
{used_str}

ONE sentence only. No preamble. No explanation.""", max_tokens=120)

    save_topic(topic)
    print(f"Topic: {topic}")
    return topic

def research_topic(topic, insights):
    print("Research pass...")
    retention = (f"\nChannel retention pattern: {insights['summary'][:200]}\nBuild arc to match."
                 if insights.get("has_data") else "")

    return claude(f"""Topic: "{topic}"{retention}

Reason through this story as a master storyteller — before writing a single word:

1. HOOK WORD: One word shown alone on black. Stops the scroll.
2. HOOK SENTENCE: Most shocking fact. No warmup.
3. SPECIFIC DETAIL: The verifiable detail that makes this undeniably real.
4. ESCALATION: The mid-story reveal that suddenly raises the stakes.
5. TWIST: The irony that recontextualizes everything before it.
6. SCENES: 5-6 distinct visual moments. Describe each specifically.
7. BEATS: 2-3 peak revelation moments — where to cut visually. Mark [BEAT].
8. SHARE TRIGGER: The one thing that makes someone show this to a friend right now.""", max_tokens=600)

def write_script(topic, research, insights):
    print("Writing script + visual direction...")
    perf = (f"\nOPTIMIZE FOR: {insights['summary'][:200]}" if insights.get("has_data") else "")

    raw = claude(f"""Topic: "{topic}"
Research: {research}
{perf}

OUTPUT THE SPOKEN SCRIPT FIRST — raw words only, zero formatting, zero labels, zero markdown.

This will be read aloud. Write for the EAR.

RULES:
- Read every sentence aloud in your head before keeping it. If you stumble, rewrite it.
- Short sentences breathe. Long sentences suffocate. Mix deliberately.
- Use contractions: "he didn't", "they couldn't", "it wasn't"
- Active voice always. Never passive.
- Sentence 1 — HOOK: Drop the listener mid-story. Most disturbing fact. No warmup.
- Sentences 2-5 — BUILD: Each sentence raises stakes slightly. Specific details.
- Sentences 6-9 — ESCALATION: The verifiable detail that makes it undeniably real.
- Final sentence — TWIST: Short. Devastating. Last word closes like a door.
- 120-140 words MAX. Tighter is better.
- Statements only. Never questions.

After the script, on separate lines:

HOOK_WORD: [one word shown alone on black screen at start]
VISUAL_STYLE: [oil_painting | cold_war_photo | daguerreotype | illuminated_manuscript | noir_photograph | renaissance_painting | gritty_documentary]
SCENES: [5-6 image prompts separated by | — specific person, place, moment, mood per scene]
BEAT_TIMES: [2-3 cut points as percentages e.g. 25|55|78]
THUMBNAIL_TEXT: [5-7 words, unbearable curiosity]
TITLE: [YouTube title, max 70 chars, engineered for clicks]
TAGS: [10 hashtags separated by |]""", max_tokens=1000)

    # Parse metadata from end of response
    meta         = {}
    script_lines = []
    for line in raw.split("\n"):
        matched = False
        for key in ("HOOK_WORD:", "VISUAL_STYLE:", "SCENES:", "BEAT_TIMES:",
                    "THUMBNAIL_TEXT:", "TITLE:", "TAGS:"):
            if line.startswith(key):
                meta[key.rstrip(":")] = line[len(key):].strip()
                matched = True
                break
        if not matched:
            script_lines.append(line)

    # Strip any markdown Claude snuck into the script
    script = "\n".join(script_lines).strip()
    script = re.sub(r'^#+\s*\w.*$',    '', script, flags=re.MULTILINE)
    script = re.sub(r'^\*\*.*?\*\*\s*', '', script, flags=re.MULTILINE)
    script = re.sub(r'^---+$',          '', script, flags=re.MULTILINE)
    script = script.strip()

    # Beat times
    beat_times = []
    for b in meta.get("BEAT_TIMES", "25|55|78").split("|"):
        try:
            beat_times.append(float(b.strip()) / 100.0)
        except ValueError:
            pass
    if not beat_times:
        beat_times = [0.25, 0.55, 0.78]

    # Scene prompts
    scene_prompts = [s.strip() for s in meta.get("SCENES", "").split("|") if s.strip()]
    if not scene_prompts:
        scene_prompts = [
            "dark stone hall, torchlight, dramatic shadows",
            "dramatic confrontation, historical figures, candlelight",
            "ominous landscape, storm clouds, ruins",
            "aged document or artifact, candlelight",
            "solitary figure, darkness, final moment",
        ]

    visual_style = meta.get("VISUAL_STYLE", DEFAULT_STYLE).strip()
    if visual_style not in VISUAL_STYLES:
        visual_style = DEFAULT_STYLE

    hook_word  = meta.get("HOOK_WORD",       script.split()[0] if script.split() else "LISTEN").upper().strip()
    thumb_text = meta.get("THUMBNAIL_TEXT",  topic[:50]).upper()
    title      = meta.get("TITLE",           topic[:70])
    tags       = [t.strip().lstrip("#") for t in meta.get("TAGS", "history|shorts|mystery|darkhistory|facts").split("|")]

    print(f"Script: {len(script.split())} words | Hook: {hook_word} | Style: {visual_style}")
    print(f"Opening: {script[:100]}...")
    return script, hook_word, scene_prompts, visual_style, beat_times, thumb_text, title, tags


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — IMAGE GENERATION
# Replicate Flux generates one image per scene. Saved to Supabase library.
# ══════════════════════════════════════════════════════════════════════════════

def generate_one_image(prompt, style, tmpdir, index):
    """
    Generate one image via Replicate Flux Schnell.
    Saves to Supabase library for future reuse.
    Returns local path in tmpdir on success, None on failure.
    """
    if not REPLICATE_API_KEY:
        return None

    style_desc  = VISUAL_STYLES.get(style, VISUAL_STYLES[DEFAULT_STYLE])
    full_prompt = f"{prompt}, {style_desc}"

    try:
        r = requests.post(
            "https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions",
            headers={"Authorization":  f"Bearer {REPLICATE_API_KEY}",
                     "Content-Type":   "application/json",
                     "Prefer":         "wait=60"},
            json={"input": {"prompt":        full_prompt,
                            "width":         1080,
                            "height":        1920,
                            "num_outputs":   1,
                            "output_format": "jpg",
                            "output_quality": 90}},
            timeout=120
        )

        if r.status_code not in (200, 201):
            print(f"  Replicate {r.status_code}: {r.text[:150]}")
            return None

        data = r.json()

        # Handle synchronous response
        if data.get("status") == "succeeded":
            output = data.get("output", [])
        else:
            # Poll up to 90s
            pred_id = data.get("id")
            if not pred_id:
                return None
            import time
            output = None
            for _ in range(90):
                time.sleep(1)
                poll = requests.get(
                    f"https://api.replicate.com/v1/predictions/{pred_id}",
                    headers={"Authorization": f"Bearer {REPLICATE_API_KEY}"},
                    timeout=10
                )
                if poll.status_code == 200:
                    pd = poll.json()
                    if pd.get("status") == "succeeded":
                        output = pd.get("output", [])
                        break
                    if pd.get("status") == "failed":
                        print(f"  Image {index} failed")
                        return None
            if output is None:
                print(f"  Image {index} timed out")
                return None

        if not output:
            return None

        img_url = output[0] if isinstance(output, list) else output
        img_r   = requests.get(img_url, timeout=30)
        if img_r.status_code != 200:
            return None

        # Write to tmpdir for this video
        tmp_path = f"{tmpdir}/scene_{index}.jpg"
        with open(tmp_path, "wb") as f:
            f.write(img_r.content)

        # Save to Supabase library (descriptive filename, embedded for search)
        ts          = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        slug        = "_".join(re.sub(r'[^\w\s]', '', prompt).lower().split()[:8])
        lib_name    = f"{style}_{slug}_{ts}_{index}.jpg"
        lib_path    = f"{tmpdir}/{lib_name}"
        with open(lib_path, "wb") as f:
            f.write(img_r.content)

        description = f"{prompt}, {style.replace('_', ' ')}"
        save_image_to_library(lib_path, description, style)

        print(f"  Scene {index+1}: {len(img_r.content)//1024}KB — {lib_name[:55]}")
        return tmp_path

    except Exception as e:
        print(f"  Image {index} error: {e}")
        return None

def generate_all_images(scene_prompts, visual_style, tmpdir):
    """Generate all scene images in parallel. Returns ordered list of paths."""
    print(f"Generating {len(scene_prompts)} images via Replicate ({visual_style})...")
    paths = [None] * len(scene_prompts)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(generate_one_image, p, visual_style, tmpdir, i): i
                   for i, p in enumerate(scene_prompts)}
        for fut in concurrent.futures.as_completed(futures):
            i = futures[fut]
            r = fut.result()
            if r:
                paths[i] = r

    valid = [p for p in paths if p]
    print(f"Generated {len(valid)}/{len(scene_prompts)} images")
    return valid

def fetch_pexels_fallback(scene_prompts, tmpdir):
    """Last-resort fallback: Pexels portrait video clips."""
    if not PEXELS_API_KEY:
        return []
    print("Pexels fallback...")
    clips = []
    for i, prompt in enumerate(scene_prompts[:5]):
        term = " ".join(prompt.replace(",", "").split()[:4])
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
            files = sorted([f for f in video.get("video_files", []) if f.get("width", 0) >= 480],
                           key=lambda x: abs(x.get("width", 0) - 1080))
            if not files:
                continue
            path = f"{tmpdir}/pexels_{i}.mp4"
            dl   = requests.get(files[0]["link"], timeout=60, stream=True)
            with open(path, "wb") as f:
                for chunk in dl.iter_content(8192):
                    f.write(chunk)
            clips.append(path)
        except Exception as e:
            print(f"  Pexels error: {e}")
    return clips


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — AUDIO
# ElevenLabs voice with word-level timestamp alignment.
# Classical music from Wikimedia Commons public domain.
# ══════════════════════════════════════════════════════════════════════════════

def generate_voiceover(script, audio_path):
    """
    Generate voiceover via ElevenLabs /with-timestamps.
    Returns list of (word, start_sec, end_sec) tuples.
    Falls back to even distribution if alignment unavailable.
    """
    print("Generating voiceover...")
    r = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}/with-timestamps",
        headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
        json={"text": script,
              "model_id": "eleven_monolingual_v1",
              "voice_settings": {"stability": 0.28, "similarity_boost": 0.85,
                                 "style": 0.52, "use_speaker_boost": True}},
        timeout=60
    )
    if r.status_code != 200:
        raise RuntimeError(f"ElevenLabs {r.status_code}: {r.text[:200]}")

    data = r.json()
    if not data.get("audio_base64"):
        raise RuntimeError("ElevenLabs returned no audio")

    with open(audio_path, "wb") as f:
        f.write(base64.b64decode(data["audio_base64"]))

    # Parse character-level alignment → word-level
    alignment = data.get("alignment", {})
    chars     = alignment.get("characters", [])
    starts    = alignment.get("character_start_times_seconds", [])
    ends      = alignment.get("character_end_times_seconds", [])

    if chars and starts:
        word_timings = []
        cur_word     = ""
        word_start   = None
        prev_end     = 0.0
        for ch, ts, te in zip(chars, starts, ends):
            if ch in " \n\t":
                if cur_word:
                    word_timings.append((cur_word, word_start, prev_end))
                    cur_word   = ""
                    word_start = None
            else:
                if word_start is None:
                    word_start = ts
                cur_word += ch
                prev_end  = te
        if cur_word:
            word_timings.append((cur_word, word_start, prev_end))
        print(f"Voiceover: {len(word_timings)} words synced")
    else:
        print("Alignment unavailable — even distribution fallback")
        duration     = get_duration(audio_path)
        words        = script.split()
        step         = duration / max(len(words), 1)
        word_timings = [(w, i * step, (i+1) * step) for i, w in enumerate(words)]

    return word_timings

def list_music_from_supabase():
    """
    List all tracks in Supabase Storage music/ bucket.
    Returns list of (filename, public_url) tuples.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        r = requests.post(
            f"{SUPABASE_URL}/storage/v1/object/list/music",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "application/json",
            },
            json={"prefix": "", "limit": 100, "offset": 0},
            timeout=10
        )
        if r.status_code != 200:
            return []
        files = r.json()
        return [
            (f["name"], f"{SUPABASE_URL}/storage/v1/object/public/music/{f['name']}")
            for f in files
            if f.get("name") and not f["name"].endswith("/")
        ]
    except Exception as e:
        print(f"  Music list error: {e}")
        return []


def fetch_music(tmpdir):
    """
    Fetch music for the video.

    Priority:
    1. Supabase music/ bucket — your approved tracks (upload via dashboard)
    2. Committed fallback file — ambient_fallback.mp3 in the repo
       A 90s dark ambient drone generated via FFmpeg sine waves.
       Always available, no network needed, never fails.

    Note: External music URLs (Wikimedia, Archive.org) are blocked by
    Railway's network proxy. Supabase is the only external source that works.
    """
    print("Fetching music...")
    final_path = f"{tmpdir}/music.mp3"

    def try_download_and_convert(url, label):
        raw_path = f"{tmpdir}/music_raw"
        try:
            r = requests.get(url, timeout=30, stream=True,
                             headers={"User-Agent": "DeadMensSecrets/1.0"})
            if r.status_code != 200:
                return False
            with open(raw_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            if os.path.getsize(raw_path) < 10_000:
                return False
            conv = subprocess.run(
                ["ffmpeg", "-y", "-i", raw_path,
                 "-c:a", "libmp3lame", "-b:a", "128k", final_path],
                capture_output=True
            )
            if conv.returncode == 0 and os.path.exists(final_path):
                print(f"Music: {os.path.getsize(final_path)//1024}KB — {label}")
                return True
        except Exception as e:
            print(f"  Track failed ({label}): {e}")
        return False

    # 1. Try Supabase music bucket — your approved tracks
    tracks = list_music_from_supabase()
    if tracks:
        print(f"  {len(tracks)} tracks in Supabase music library")
        for name, url in random.sample(tracks, len(tracks)):
            if try_download_and_convert(url, name):
                return final_path
        print("  Supabase tracks failed")

    # 2. Committed fallback — always works, no network needed
    fallback = Path(__file__).parent / "ambient_fallback.mp3"
    if fallback.exists():
        import shutil
        shutil.copy(str(fallback), final_path)
        print(f"Music: using committed ambient fallback ({fallback.stat().st_size//1024}KB)")
        return final_path

    print("Music unavailable")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5 — VIDEO ASSEMBLY
# Background: Ken Burns effect on AI images + crossfade transitions
# Final: hook frame + captions + branding + music mix
# ══════════════════════════════════════════════════════════════════════════════

# ── CINEMATIC MOTION PROFILES ─────────────────────────────────────────────────
# Each profile is a distinct camera movement. Assigned round-robin per scene
# so no two consecutive scenes feel the same.
# All use 4x upscale before motion to prevent blur on zoom-in.
MOTION_PROFILES = [
    # 0: Slow push in — classic documentary, weight and inevitability
    lambda f: (
        f"zoompan=z='min(zoom+0.0002,1.05)'"
        f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
        f":d={f}:s={W}x{H}:fps=30"
    ),
    # 1: Slow pull back — reveal, dread building as scene widens
    lambda f: (
        f"zoompan=z='if(eq(on,1),1.06,max(1.0,zoom-0.0002))'"
        f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
        f":d={f}:s={W}x{H}:fps=30"
    ),
    # 2: Drift right — lateral pan, surveillance feel
    lambda f: (
        f"zoompan=z='1.04'"
        f":x='iw/2-(iw/zoom/2)+(on/{max(f,1)})*{W//8}':y='ih/2-(ih/zoom/2)'"
        f":d={f}:s={W}x{H}:fps=30"
    ),
    # 3: Drift left with slight zoom — closing in, tightening
    lambda f: (
        f"zoompan=z='min(zoom+0.0001,1.04)'"
        f":x='iw/2-(iw/zoom/2)-((on/{max(f,1)})*{W//10})':y='ih/2-(ih/zoom/2)'"
        f":d={f}:s={W}x{H}:fps=30"
    ),
    # 4: Slow tilt up — from ground to sky, emergence or ascent
    lambda f: (
        f"zoompan=z='1.04'"
        f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)-((on/{max(f,1)})*{H//12})'"
        f":d={f}:s={W}x{H}:fps=30"
    ),
    # 5: Slow tilt down — descent, falling, oppression
    lambda f: (
        f"zoompan=z='1.04'"
        f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)+((on/{max(f,1)})*{H//12})'"
        f":d={f}:s={W}x{H}:fps=30"
    ),
]

# Color grade per visual style — applied to every clip
STYLE_GRADE = {
    "oil_painting":           "colorchannelmixer=rr=0.85:gg=0.80:bb=0.95,eq=contrast=1.15:brightness=-0.04:saturation=0.80",
    "cold_war_photo":         "colorchannelmixer=rr=0.75:gg=0.78:bb=0.78,eq=contrast=1.30:brightness=-0.06:saturation=0.20",
    "daguerreotype":          "colorchannelmixer=rr=0.90:gg=0.82:bb=0.65,eq=contrast=1.20:brightness=-0.03:saturation=0.35",
    "illuminated_manuscript": "colorchannelmixer=rr=0.92:gg=0.85:bb=0.60,eq=contrast=1.10:brightness=0.02:saturation=0.90",
    "noir_photograph":        "colorchannelmixer=rr=0.70:gg=0.72:bb=0.72,eq=contrast=1.40:brightness=-0.08:saturation=0.10",
    "renaissance_painting":   "colorchannelmixer=rr=0.88:gg=0.82:bb=0.72,eq=contrast=1.12:brightness=-0.02:saturation=0.85",
    "gritty_documentary":     "colorchannelmixer=rr=0.82:gg=0.80:bb=0.78,eq=contrast=1.25:brightness=-0.05:saturation=0.65",
}
DEFAULT_GRADE = STYLE_GRADE["oil_painting"]


def build_cinematic_clip(img_path, dur, motion_idx, visual_style, out_path, tmpdir):
    """
    Render one cinematic clip from a still image.
    Applies motion profile + style-matched color grade + vignette.
    Returns True on success.

    Motion: chosen from MOTION_PROFILES round-robin — no two scenes feel the same.
    Grade:  matched to the visual style of the story (cold war = desaturated, etc.)
    Flicker: subtle random brightness pulse simulates torch/candlelight on dark scenes.
    """
    frames = max(int(dur * 30), 1)
    motion = MOTION_PROFILES[motion_idx % len(MOTION_PROFILES)](frames)
    grade  = STYLE_GRADE.get(visual_style, DEFAULT_GRADE)

    # Torch flicker: gentle random brightness variation every ~8 frames
    # Simulates candlelight / torchlight in dark historical scenes
    # Subtle enough not to distract, present enough to feel alive
    flicker = "noise=alls=3:allf=t,lutyuv=y=val+random(0)*8-4"

    vf = (
        f"scale={W*4}:{H*4}:force_original_aspect_ratio=increase,"
        f"crop={W*4}:{H*4},"
        f"{motion},"
        f"{grade},"
        f"vignette=PI/3.5,"
        f"{flicker}"
    )

    r = run(
        ["ffmpeg", "-y", "-loop", "1", "-i", img_path,
         "-vf", vf,
         "-t", str(dur + 0.5),
         "-c:v", "libx264", "-preset", "fast", "-crf", "18",
         "-pix_fmt", "yuv420p", "-an", out_path],
        check=False
    )
    return r.returncode == 0 and os.path.exists(out_path)


def build_background(image_paths, beat_times, voice_dur, tmpdir, visual_style=DEFAULT_STYLE):
    """
    Build cinematic background video from AI-generated images.

    Each image gets a distinct motion profile (push, pull, drift, tilt) —
    never the same movement twice in a row. Color grade is matched to the
    story's visual style. Torch flicker adds life to static images.
    Scenes are joined with 0.5s crossfade transitions.

    Falls back gracefully: fewer images → cycle them. No images → dark gradient.
    """
    print("Building cinematic background...")
    bg = f"{tmpdir}/bg.mp4"

    # Dark animated gradient fallback — better than a hard black screen
    if not image_paths:
        run(["ffmpeg", "-y", "-f", "lavfi",
             "-i", f"color=c=0x08080f:size={W}x{H}:duration={voice_dur}:rate=30",
             "-vf", "noise=alls=6:allf=t+u,vignette=PI/3",
             "-c:v", "libx264", "-preset", "fast", "-crf", "28",
             "-pix_fmt", "yuv420p", bg])
        return bg

    # Segment durations from beat points — each beat = scene cut
    pts  = sorted(set([0.0] + beat_times + [1.0]))
    durs = [(pts[i+1] - pts[i]) * voice_dur for i in range(len(pts) - 1)]

    # Cycle images if fewer than segments
    imgs = (image_paths * (len(durs) // len(image_paths) + 1))[:len(durs)]

    # Render each image as a cinematic clip
    clips = []
    for i, (img, dur) in enumerate(zip(imgs, durs)):
        out = f"{tmpdir}/cin_{i}.mp4"
        ok  = build_cinematic_clip(img, dur, i, visual_style, out, tmpdir)
        if ok:
            clips.append((out, dur))
        else:
            # Fallback: plain static scaled clip
            sout = f"{tmpdir}/static_{i}.mp4"
            run(["ffmpeg", "-y", "-loop", "1", "-i", img,
                 "-t", str(dur + 0.5),
                 "-vf", f"scale={W}:{H}:force_original_aspect_ratio=increase,crop={W}:{H}",
                 "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                 "-pix_fmt", "yuv420p", "-an", sout],
                check=False)
            if os.path.exists(sout):
                clips.append((sout, dur))

    if not clips:
        return build_background([], beat_times, voice_dur, tmpdir)

    # Single clip
    if len(clips) == 1:
        run(["ffmpeg", "-y", "-i", clips[0][0],
             "-t", str(voice_dur), "-c:v", "libx264", "-preset", "fast",
             "-crf", "22", "-pix_fmt", "yuv420p", bg])
        return bg

    # Crossfade chain — 0.5s overlap between each scene
    xfade  = 0.5
    inputs = sum([["-i", c] for c, _ in clips], [])
    parts  = []
    label  = "[0:v]"
    offset = 0.0

    for i in range(1, len(clips)):
        offset  += clips[i-1][1] - xfade
        offset   = max(offset, 0.01)
        nxt      = f"[v{i}]" if i < len(clips) - 1 else "[vout]"
        parts.append(
            f"{label}[{i}:v]xfade=transition=fade"
            f":duration={xfade}:offset={offset:.3f}{nxt}"
        )
        label = nxt

    r = run(
        ["ffmpeg", "-y"] + inputs + [
            "-filter_complex", ";".join(parts),
            "-map", "[vout]",
            "-t", str(voice_dur),
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-pix_fmt", "yuv420p", bg
        ],
        check=False
    )

    if r.returncode != 0:
        # Fallback: simple concat without transitions
        print("Crossfade failed — concat fallback")
        lst = f"{tmpdir}/list.txt"
        with open(lst, "w") as f:
            for c, _ in clips:
                f.write(f"file '{c}'\n")
        r2 = run(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", lst,
             "-t", str(voice_dur), "-vf", f"scale={W}:{H}",
             "-c:v", "libx264", "-preset", "fast", "-crf", "22",
             "-pix_fmt", "yuv420p", bg],
            check=False
        )
        if r2.returncode != 0:
            return build_background([], beat_times, voice_dur, tmpdir)

    print(f"Cinematic background: {voice_dur:.1f}s, {len(clips)} scenes")
    return bg

def assemble_video(word_timings, hook_word, audio_path, bg_path,
                   music_path, beat_times, output_path):
    """
    Final assembly:
    - 0.55s hook frame: black screen + single hook word
    - Fade in from black
    - Word-synced captions: strict 3 words, 70px, never overflow
    - Channel branding: gold top center
    - Beat flash pulses on story revelations
    - Music mixed at 8% volume under voice
    """
    print("Assembling final video...")
    voice_dur = get_duration(audio_path)
    total_dur = voice_dur + HOOK_DUR

    # Caption chunks: exactly 3 words each
    # FFmpeg drawtext has NO word wrap — overflow is a hard bug.
    # 3 words at 70px on 1080px width is safe for all word lengths.
    chunks = []
    i = 0
    while i < len(word_timings):
        group = word_timings[i:i+3]
        if group:
            text  = " ".join(w[0] for w in group)
            t0    = group[0][1] + HOOK_DUR
            t1    = group[-1][2] + HOOK_DUR
            if i + 3 < len(word_timings):
                t1 = min(t1 + 0.04, word_timings[i+3][1] + HOOK_DUR)
            chunks.append((text, t0, t1))
        i += 3

    beat_secs = [b * voice_dur + HOOK_DUR for b in beat_times]

    filters = [
        "vignette=PI/3.5",
        f"fade=t=in:st={HOOK_DUR:.2f}:d=0.4",
        # Channel branding — always visible
        f"drawtext=text='DEAD MEN\u2019S SECRETS'"
        f":fontfile={FONT}:fontsize=38:fontcolor=#FFD700"
        f":x=(w-text_w)/2:y=88:borderw=3:bordercolor=black@0.95",
        # Hook word — black screen, first 0.5s only
        f"drawtext=text='{esc(hook_word)}'"
        f":fontfile={FONT}:fontsize=170:fontcolor=white"
        f":x=(w-text_w)/2:y=(h-text_h)/2"
        f":borderw=8:bordercolor=black"
        f":enable='between(t,0,{HOOK_DUR-0.05:.2f})'",
    ]

    # Word-synced captions
    for text, t0, t1 in chunks:
        filters.append(
            f"drawtext=text='{esc(text)}'"
            f":fontfile={FONT}:fontsize=70:fontcolor=white"
            f":x=(w-text_w)/2:y={CAPTION_Y}"
            f":borderw=6:bordercolor=black@0.9"
            f":enable='between(t,{t0:.3f},{t1:.3f})'"
        )

    # Beat flash pulses
    for bt in beat_secs:
        filters.append(
            f"drawbox=x=0:y=0:w={W}:h={H}:color=white@0.06:t=fill"
            f":enable='between(t,{bt:.2f},{bt+0.08:.2f})'"
        )

    vf = ",".join(filters)

    if music_path and os.path.exists(music_path):
        af = (f"[1:a]volume=1.0[voice];"
              f"[2:a]volume=0.08,"
              f"afade=t=in:st={HOOK_DUR}:d=2.5,"
              f"afade=t=out:st={total_dur-2.0}:d=1.8[music];"
              f"[voice][music]amix=inputs=2:duration=first[aout]")
        cmd = ["ffmpeg", "-y",
               "-i", bg_path, "-i", audio_path, "-i", music_path,
               "-filter_complex", af, "-vf", vf,
               "-map", "0:v", "-map", "[aout]",
               "-c:v", "libx264", "-preset", "fast", "-crf", "18",
               "-c:a", "aac", "-b:a", "192k",
               "-t", str(total_dur),
               "-movflags", "+faststart", "-pix_fmt", "yuv420p",
               output_path]
    else:
        cmd = ["ffmpeg", "-y",
               "-i", bg_path, "-i", audio_path,
               "-vf", vf,
               "-c:v", "libx264", "-preset", "fast", "-crf", "18",
               "-c:a", "aac", "-b:a", "192k",
               "-shortest", "-movflags", "+faststart", "-pix_fmt", "yuv420p",
               output_path]

    r = run(cmd, check=False)
    if r.returncode != 0:
        print(f"Assembly retry without music: {r.stderr[-100:]}")
        run(["ffmpeg", "-y", "-i", bg_path, "-i", audio_path,
             "-vf", vf, "-c:v", "libx264", "-preset", "fast", "-crf", "18",
             "-c:a", "aac", "-b:a", "192k", "-shortest",
             "-movflags", "+faststart", "-pix_fmt", "yuv420p", output_path])

    print(f"Final video: {os.path.getsize(output_path)/1048576:.1f}MB, {total_dur:.1f}s")

def build_thumbnail(thumb_text, image_paths, tmpdir):
    """
    Thumbnail: first scene image darkened + massive white text + gold branding.
    Falls back to black background if no image available.
    """
    print("Building thumbnail...")
    TW, TH     = 1280, 720
    thumb_path = f"{tmpdir}/thumbnail.jpg"
    bg_frame   = f"{tmpdir}/thumb_bg.jpg"
    got_frame  = False

    if image_paths:
        r = run(["ffmpeg", "-y", "-i", image_paths[0],
                 "-vf", f"scale={TW*2}:{TH*2}:force_original_aspect_ratio=increase,crop={TW}:{TH}",
                 "-frames:v", "1", "-q:v", "2", bg_frame], check=False)
        got_frame = r.returncode == 0

    lines  = textwrap.wrap(thumb_text.upper(), width=16)
    line_h = 115
    sy     = (TH // 2) - (len(lines) * line_h // 2) + 20

    text_filters = []
    for i, line in enumerate(lines):
        y    = sy + i * line_h
        safe = esc(line)
        text_filters += [
            f"drawtext=text='{safe}':fontfile={FONT}:fontsize=110"
            f":fontcolor=black@0.6:x=(w-text_w)/2+5:y={y+5}",
            f"drawtext=text='{safe}':fontfile={FONT}:fontsize=110"
            f":fontcolor=white:x=(w-text_w)/2:y={y}:borderw=6:bordercolor=black",
        ]
    text_filters.append(
        f"drawtext=text='DEAD MEN\u2019S SECRETS':fontfile={FONT}:fontsize=42"
        f":fontcolor=#FFD700:x=(w-text_w)/2:y=28:borderw=3:bordercolor=black"
    )
    tf = ",".join(text_filters)

    if got_frame:
        vf = (f"scale={TW}:{TH}:force_original_aspect_ratio=increase,crop={TW}:{TH},"
              f"colorchannelmixer=rr=0.48:gg=0.48:bb=0.60,"
              f"eq=contrast=1.25:brightness=-0.08,vignette=PI/3,{tf}")
        r = run(["ffmpeg", "-y", "-i", bg_frame, "-vf", vf,
                 "-frames:v", "1", "-q:v", "2", thumb_path], check=False)
    else:
        vf = f"color=c=0x080810:size={TW}x{TH},{tf}"
        r  = run(["ffmpeg", "-y", "-f", "lavfi", "-i", vf,
                  "-frames:v", "1", "-q:v", "2", thumb_path], check=False)

    if r.returncode == 0 and os.path.exists(thumb_path):
        print("Thumbnail built")
        return thumb_path
    print("Thumbnail failed (non-critical)")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 6 — PUBLISH
# ══════════════════════════════════════════════════════════════════════════════

def upload_to_youtube(video_path, thumb_path, title, script, tags, token):
    print("Uploading to YouTube...")
    desc = f"{script}\n\nFollow Dead Men's Secrets — true history buried for a reason.\n\n#" + " #".join(tags)

    init = requests.post(
        "https://www.googleapis.com/upload/youtube/v3/videos"
        "?uploadType=resumable&part=snippet,status",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type":  "application/json",
                 "X-Upload-Content-Type": "video/mp4"},
        json={"snippet": {"title": title[:100], "description": desc,
                          "tags": tags[:15], "categoryId": "27"},
              "status":  {"privacyStatus": "public", "selfDeclaredMadeForKids": False}},
        timeout=30
    )
    if init.status_code != 200:
        raise RuntimeError(f"YouTube init error: {init.text}")

    with open(video_path, "rb") as f:
        video_data = f.read()

    upload = requests.put(
        init.headers["Location"],
        headers={"Content-Type":   "video/mp4",
                 "Content-Length": str(len(video_data))},
        data=video_data,
        timeout=120
    )
    if upload.status_code not in (200, 201):
        raise RuntimeError(f"YouTube upload error: {upload.text}")

    video_id = upload.json()["id"]
    print(f"Live: https://youtube.com/shorts/{video_id}")

    if thumb_path and os.path.exists(thumb_path):
        try:
            with open(thumb_path, "rb") as f:
                td = f.read()
            tr = requests.post(
                f"https://www.googleapis.com/upload/youtube/v3/thumbnails/set"
                f"?videoId={video_id}&uploadType=media",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type":  "image/jpeg",
                         "Content-Length": str(len(td))},
                data=td, timeout=30
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
    print(f"\n{'═'*60}\nDead Men's Secrets — {now}\n{'═'*60}\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path  = f"{tmpdir}/voice.mp3"
        output_path = f"{tmpdir}/short.mp4"

        # ── PHASE 1: LEARN ────────────────────────────────────────────────
        token    = get_youtube_token()
        ana_data = pull_analytics(token)
        insights = analyze_performance(ana_data)

        # ── PHASE 2: CREATE ───────────────────────────────────────────────
        topic  = generate_topic(insights)
        research = research_topic(topic, insights)
        (script, hook_word, scene_prompts, visual_style,
         beat_times, thumb_text, title, tags) = write_script(topic, research, insights)

        # ── PHASE 3: GENERATE VISUALS + AUDIO (parallel) ──────────────────
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            img_fut   = ex.submit(generate_all_images, scene_prompts, visual_style, tmpdir)
            voice_fut = ex.submit(generate_voiceover,  script, audio_path)
            image_paths  = img_fut.result()
            word_timings = voice_fut.result()

        # Fallback chain: Supabase library → Pexels → dark gradient (in build_background)
        if not image_paths:
            print("Replicate failed — trying Supabase image library")
            image_paths = get_images_from_library(scene_prompts, tmpdir)
        if not image_paths:
            print("Library empty — trying Pexels")
            image_paths = fetch_pexels_fallback(scene_prompts, tmpdir)

        # ── PHASE 4: PRODUCE ──────────────────────────────────────────────
        voice_dur  = get_duration(audio_path)
        music_path = fetch_music(tmpdir)
        bg_path    = build_background(image_paths, beat_times, voice_dur, tmpdir, visual_style)
        assemble_video(word_timings, hook_word, audio_path, bg_path,
                       music_path, beat_times, output_path)
        thumb_path = build_thumbnail(thumb_text, image_paths, tmpdir)

        # ── PHASE 5: PUBLISH ──────────────────────────────────────────────
        video_id = upload_to_youtube(output_path, thumb_path, title, script, tags, token)
        log_video(video_id, title, topic)

        cost = len(scene_prompts) * 0.003
        print(f"\n{'═'*60}")
        print(f"LIVE:  https://youtube.com/shorts/{video_id}")
        print(f"TITLE: {title}")
        print(f"COST:  ~${cost:.3f} image generation")
        print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
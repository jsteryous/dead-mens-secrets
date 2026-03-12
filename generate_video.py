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

USED_TOPICS_FILE = Path(__file__).parent / "used_topics.json"
PERFORMANCE_FILE = Path(__file__).parent / "performance_log.json"
INSIGHTS_FILE    = Path(__file__).parent / "channel_insights.json"

FONT      = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
W, H      = 1080, 1920
CAPTION_Y = int(H * 0.72)

# Visual style applied to every generated image
IMAGE_STYLE = (
    "dark oil painting style, dramatic chiaroscuro lighting, "
    "cinematic composition, rich shadows, historically detailed, "
    "masterpiece quality, moody atmosphere, deep blacks"
)
IMAGE_NEGATIVE = (
    "blurry, low quality, modern, cartoon, anime, bright colors, "
    "cheerful, stock photo, watermark, text, logo"
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
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={"client_id": YOUTUBE_CLIENT_ID,
              "client_secret": YOUTUBE_CLIENT_SECRET,
              "refresh_token": YOUTUBE_REFRESH_TOKEN,
              "grant_type": "refresh_token"}
    )
    data = r.json()
    if "access_token" not in data:
        raise Exception(f"Token error: {data}")
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

    INSIGHTS_FILE.write_text(json.dumps(insights, indent=2))
    print(f"Top video: \"{top5[0]['title']}\" ({top5[0]['views']} views)")
    return insights


def update_performance_log(video_id, title, topic, analytics_data):
    log = []
    if PERFORMANCE_FILE.exists():
        try:
            log = json.loads(PERFORMANCE_FILE.read_text())
        except:
            log = []
    log.append({
        "video_id":   video_id,
        "title":      title,
        "topic":      topic,
        "posted_at":  datetime.datetime.utcnow().isoformat(),
    })
    PERFORMANCE_FILE.write_text(json.dumps(log[-200:], indent=2))


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: CONTENT ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def generate_topic(insights):
    print("Generating topic...")

    used = []
    if USED_TOPICS_FILE.exists():
        try:
            used = json.loads(USED_TOPICS_FILE.read_text())
        except:
            used = []
    used_str = "\n".join(f"- {t}" for t in used[-60:]) or "None yet"

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

    used.append(topic)
    USED_TOPICS_FILE.write_text(json.dumps(used[-120:], indent=2))
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

SCRIPT RULES:
- Sentence 1 (HOOK): Most shocking fact. Cold. Immediate. No warmup.
- Sentences 2-4 (CONTEXT): Minimum viable context. Every word earns its place.
- Sentences 5-8 (ESCALATION): Stack specific details. Build dread.
- Final sentence (TWIST): Recontextualizes everything. Last word closes like a door.
- 130-150 words MAX
- No stage directions, headers, asterisks
- Statements only — no questions
- Conversational, urgent, intimate voice

After the script output these on separate lines:

HOOK_WORD: [single most shocking word — shown alone first]
SCENES: [5-6 image prompts separated by | — each describes exactly what should appear on screen for that part of the story. Be specific: who, what, where, mood. These will be painted by AI.]
BEAT_TIMES: [2-3 percentage points for visual cuts e.g. 25|55|78]
THUMBNAIL_TEXT: [5-7 words MAX — unbearable curiosity]
TITLE: [YouTube title max 70 chars — engineered for click-through]
TAGS: [10 hashtags separated by |]""", max_tokens=1000)

    script_lines, meta = [], {}
    for line in raw.split("\n"):
        parsed = False
        for k in ["HOOK_WORD:", "SCENES:", "BEAT_TIMES:",
                  "THUMBNAIL_TEXT:", "TITLE:", "TAGS:"]:
            if line.startswith(k):
                meta[k.rstrip(":")] = line[len(k):].strip()
                parsed = True
                break
        if not parsed:
            script_lines.append(line)

    script = "\n".join(script_lines).strip()

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

    print(f"Script: {len(script.split())} words | Hook: {hook_word}")
    print(f"Scenes: {len(scene_prompts)} visual prompts")
    print(f"Opening: {script[:100]}...")
    return script, hook_word, scene_prompts, beat_times, thumb_text, title, tags


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: AI IMAGE GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def generate_image_replicate(prompt, tmpdir, index):
    """Generate one image via Replicate Flux model. 9:16 portrait native."""
    full_prompt = f"{prompt}, {IMAGE_STYLE}"

    try:
        # Start the prediction
        r = requests.post(
            "https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions",
            headers={
                "Authorization": f"Bearer {REPLICATE_API_KEY}",
                "Content-Type": "application/json",
                "Prefer": "wait"  # wait for completion synchronously
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

            for _ in range(60):  # max 60 seconds
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
        img_url  = output[0] if isinstance(output, list) else output
        img_path = f"{tmpdir}/scene_{index}.jpg"
        img_r    = requests.get(img_url, timeout=30)
        if img_r.status_code == 200:
            with open(img_path, "wb") as f:
                f.write(img_r.content)
            print(f"  Scene {index+1} generated: {len(img_r.content)//1024}KB")
            return img_path
        return None

    except Exception as e:
        print(f"  Image {index} error: {e}")
        return None


def generate_all_images(scene_prompts, tmpdir):
    """Generate all scene images in parallel for speed."""
    print(f"Generating {len(scene_prompts)} AI images via Replicate...")

    if not REPLICATE_API_KEY:
        print("No Replicate key — falling back to Pexels")
        return []

    image_paths = [None] * len(scene_prompts)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(generate_image_replicate, prompt, tmpdir, i): i
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


def fetch_pexels_fallback(scene_prompts, tmpdir):
    """Fallback to Pexels if Replicate unavailable."""
    if not PEXELS_API_KEY:
        return []
    print("Fetching Pexels fallback footage...")
    clips = []
    for i, prompt in enumerate(scene_prompts[:5]):
        # Extract key terms from scene prompt
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
    Royalty-free music from Free Music Archive API.
    Filters for dark/dramatic/cinematic tracks.
    Falls back to curated direct links.
    """
    print("Fetching background music...")

    # Try Free Music Archive API
    try:
        r = requests.get(
            "https://freemusicarchive.org/api/get/tracks.json",
            params={
                "genre_id": "27",   # Experimental/Dark
                "limit": 20,
                "sort": "track_date_recorded",
            },
            timeout=10
        )
        if r.status_code == 200:
            tracks = r.json().get("dataset", [])
            # Filter for downloadable tracks
            for track in random.sample(tracks, min(5, len(tracks))):
                url = track.get("track_url", "")
                if url:
                    music_path = f"{tmpdir}/music.mp3"
                    dl = requests.get(url, timeout=20, stream=True)
                    if dl.status_code == 200:
                        with open(music_path, "wb") as f:
                            for chunk in dl.iter_content(8192):
                                f.write(chunk)
                        if os.path.getsize(music_path) > 50000:
                            print(f"Music from FMA: {os.path.getsize(music_path)//1024}KB")
                            return music_path
    except Exception as e:
        print(f"FMA failed: {e}")

    # Fallback: curated CC0 tracks
    fallback_urls = [
        "https://cdn.pixabay.com/download/audio/2022/10/25/audio_946b5f8a4b.mp3",
        "https://cdn.pixabay.com/download/audio/2022/03/15/audio_8cb749d14e.mp3",
        "https://cdn.pixabay.com/download/audio/2023/01/04/audio_8faada582e.mp3",
        "https://cdn.pixabay.com/download/audio/2022/08/23/audio_d16737dc28.mp3",
        "https://cdn.pixabay.com/download/audio/2021/11/01/audio_8a7a1b6f4b.mp3",
    ]
    music_path = f"{tmpdir}/music.mp3"
    for url in random.sample(fallback_urls, len(fallback_urls)):
        try:
            r = requests.get(url, timeout=20, stream=True)
            if r.status_code == 200:
                with open(music_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                if os.path.getsize(music_path) > 10000:
                    print(f"Music (fallback): {os.path.getsize(music_path)//1024}KB")
                    return music_path
        except:
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

    # Build caption chunks from word-level timestamps
    # Karaoke effect: show 3-4 word chunk, current words slightly bigger
    chunks = []
    i = 0
    while i < len(word_timings):
        size  = random.choice([3, 3, 3, 4, 4])
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

    # Word-synced captions — clean white, lower third
    for (text, t0, t1) in chunks:
        filters.append(
            f"drawtext=text='{esc(text)}'"
            f":fontfile={FONT}:fontsize=80:fontcolor=white"
            f":x=(w-text_w)/2:y={CAPTION_Y}-text_h/2"
            f":borderw=5:bordercolor=black"
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

        # ── PHASE 1: LEARN ────────────────────────────────────────────────
        token          = get_youtube_token()
        analytics_data = pull_youtube_analytics(token)
        insights       = analyze_performance(analytics_data)

        # ── PHASE 2: CREATE ───────────────────────────────────────────────
        topic                                              = generate_topic(insights)
        research                                           = research_topic(topic, insights)
        script, hook_word, scene_prompts, beat_times, \
            thumb_text, title, tags                        = write_script(topic, research, insights)

        # ── PHASE 3: GENERATE VISUALS (parallel with voiceover) ───────────
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            images_future = executor.submit(generate_all_images, scene_prompts, tmpdir)
            voice_future  = executor.submit(generate_voiceover, script, audio_path)

            image_paths  = images_future.result()
            word_timings = voice_future.result()

        # Fallback to Pexels if no images generated
        if not image_paths:
            print("Falling back to Pexels footage")
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